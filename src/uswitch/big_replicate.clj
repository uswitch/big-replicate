(ns uswitch.big-replicate
  (:require [gclouj.bigquery :as bq]
            [clojure.string :as s]
            [clojure.tools.cli :refer (parse-opts)]
            [clojure.set :as se]
            [clojure.tools.logging :refer (info error debug)]
            [clojure.core.async :as a])
  (:import [com.google.api.client.googleapis.json GoogleJsonResponseException])
  (:gen-class))

(def cli [["-s" "--source-project PROJECT_ID"      "Source Google Cloud Project"]
          ["-d" "--destination-project PROJECT_ID" "Destination Google Cloud Project"]
          ["-g" "--google-cloud-bucket BUCKET"     "Staging bucket to store exported data"]
          ["-i" "--datasets DATASETS"              "Comma-separated list of Dataset IDs to export"]
          ["-n" "--number NUMBER"                  "Number of days to look back for missing tables"
           :default 7 :parse-fn #(Integer/parseInt %)]
          ["-a" "--number-of-agents NUMBER"        "Number of concurrent replication agents to run."
           :default (.availableProcessors (Runtime/getRuntime)) :parse-fn #(Integer/parseInt %)]
          ["-h" "--help"                           "Display summary"]])

(defrecord TableReference [project-id dataset-id table-id])

(defn sources
  "Finds all tables in dataset to replicate"
  [project-id datasets]
  (let [service (bq/service {:project-id project-id})]
    (->> (bq/datasets service)
         (map :dataset-id)
         (filter (fn [dataset]
                   (let [id (get-in dataset [:dataset-id])]
                     (some (set datasets) [id]))))
         (mapcat (fn [dataset]
                   (bq/tables service dataset)))
         (map :table-id))))

(defn sessions-sources
  "Finds Google Analytics session source tables"
  [project-id datasets]
  (->> (sources project-id datasets)
       (filter (fn [table]
                 (re-matches #"ga_sessions_\d+" (:table-id table))))
       (map (fn [{:keys [project-id dataset-id table-id]}] (TableReference. project-id dataset-id table-id)))))

(defn staging-location [bucket {:keys [dataset-id table-id] :as table-reference}]
  (format "%s/%s/%s/*" bucket dataset-id table-id))






(defn extract-table [{:keys [source-table staging-bucket] :as current-state}]
  {:pre [(.startsWith staging-bucket "gs://")
         (not (.endsWith staging-bucket "/"))]}
  (let [uri (staging-location staging-bucket source-table)]
    (info "starting extract for" source-table "into" uri)
    (let [job (bq/extract-job (bq/service) source-table uri)]
      (assoc current-state
        :state       :wait-for-extract
        :extract-uri uri
        :job         job))))

(defn pending? [job]
  (= :pending (get-in job [:status :state])))

(defn failed? [job]
  (and (= :done (get-in job [:status :state]))
       (not (empty? (get-in job [:status :errors])))))

(defn poll-job [job]
  (let [{:keys [job-id]} job
        job-state        (bq/job (bq/service) job-id)]
    (cond (pending? job-state)       [:pending job-state]
          (bq/running? job-state)    [:running job-state]
          (failed? job-state)        [:failed job-state]
          (bq/successful? job-state) [:successful job-state])))

(defn wait-for-job [next-state {:keys [job] :as current-state}]
  (let [[status job] (poll-job job)]
    (cond (= :failed status)     (assoc current-state
                                   :state :failed
                                   :job   job)
          (= :successful status) (assoc current-state
                                   :state next-state)
          :else                  (do (debug "waiting for job" job)
                                     (Thread/sleep 30000)
                                     (assoc current-state :job job)))))

(def wait-for-extract (partial wait-for-job :load))
(def wait-for-load    (partial wait-for-job :completed))

(defn load-table [{:keys [destination-table source-table extract-uri dataset-location] :as current-state}]
  (let [service               (bq/service)
        table                 (bq/table service source-table)
        schema                (get-in table [:definition :schema])]
    (let [job (bq/load-job (bq/service) destination-table {:create-disposition :needed
                                                           :write-disposition  :empty
                                                           :schema             schema} [extract-uri])]
      (info "starting load into" destination-table)
      (assoc current-state
        :state :wait-for-load
        :job   job))))


(defn failed [current-state]
  (error "failed" current-state))

(defn completed [{:keys [destination-table]}]
  (info "successfully replicated" destination-table))

(defn progress [current-state]
  (loop [current-state current-state]
    (let [state      (:state current-state)
          op         ({:extract          extract-table
                       :wait-for-extract wait-for-extract
                       :failed           failed
                       :load             load-table
                       :wait-for-load    wait-for-load
                       :completed        completed} state)
          next-state (try (op current-state)
                          (catch Exception ex
                            (assoc current-state
                              :state     :failed
                              :exception ex)))]
      (when next-state
        (recur next-state)))))

(defn replicator-agent [in-ch completed-ch]
  (a/thread
    (loop [state (a/<!! in-ch)]
      (when state
        (progress state)
        (a/>!! completed-ch (select-keys state [:source-table :destination-table]))
        (recur (a/<!! in-ch))))))

(defn -main [& args]
  (let [{:keys [options summary errors]} (parse-opts args cli)]
    (when errors
      (println errors)
      (System/exit 1))
    (when (:help options)
      (println summary)
      (System/exit 0))
    (let [datasets       (-> (:datasets options) (s/split #","))
          sources        (sessions-sources (:source-project options) datasets)
          destinations   (->> (sessions-sources (:destination-project options) datasets)
                              (map (fn [table]
                                     (assoc table :project-id (:source-project options)))))
          target         (se/difference (set sources) (set destinations))
          sorted-targets (->> target (sort-by :table-id) (reverse) (take (:number options)))
          in-ch          (a/chan)
          completed-ch   (a/chan)]
      (a/thread
        (doseq [t sorted-targets]
          (let [{:keys [google-cloud-bucket destination-project dataset-location]} options
                state {:source-table      t
                       :destination-table (assoc t :project-id destination-project)
                       :staging-bucket    google-cloud-bucket
                       :state             :extract
                       :dataset-location  dataset-location}]
            (a/>!! in-ch state))))
      (let [agents (:number-of-agents options)]
        (info "creating" agents "replicator agents")
        (dotimes [_ agents]
          (replicator-agent in-ch completed-ch)))
      (loop [n 1
             m (a/<!! completed-ch)]
        (let [expected-count (count sorted-targets)]
          (when m
            (info (format "%d/%d" n expected-count) "completed," m)
            (if (= n expected-count)
              (info "finished")
              (recur (inc n) (a/<!! completed-ch)))))))))
