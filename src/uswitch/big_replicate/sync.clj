(ns uswitch.big-replicate.sync
  (:require [gclouj.bigquery :as bq]
            [clojure.string :as s]
            [clojure.tools.cli :refer (parse-opts)]
            [clojure.set :as se]
            [clojure.string :as st]
            [clojure.tools.logging :refer (info error debug)]
            [clojure.core.async :as a])
  (:import [com.google.api.client.googleapis.json GoogleJsonResponseException])
  (:gen-class))

(def cli [["-s" "--source-project PROJECT_ID"      "Source Google Cloud Project"]
          ["-i" "--source-dataset DATASET_ID"      "Source BigQuery dataset"]
          ["-p" "--destination-project PROJECT_ID" "Destination Google Cloud Project"]
          ["-d" "--destination-dataset DATASET_ID" "Destination BigQuery dataset"]
          ["-g" "--google-cloud-bucket BUCKET"     "Staging bucket to store exported data"]
          ["-n" "--number NUMBER"                  "Number of days to look back for missing tables"
           :default 7 :parse-fn #(Integer/parseInt %)]
          ["-a" "--number-of-agents NUMBER"        "Number of concurrent replication agents to run."
           :default (.availableProcessors (Runtime/getRuntime)) :parse-fn #(Integer/parseInt %)]
          ["-h" "--help"                           "Display summary"]])

(defrecord TableReference [project-id dataset-id table-id])

(defn sources
  "Finds all tables in dataset to replicate"
  [project-id dataset]
  (let [service (bq/service {:project-id project-id})]
    (->> (bq/tables service {:project-id project-id
                             :dataset-id dataset})
         (map :table-id))))

(defn sessions-sources
  "Finds Google Analytics session source tables"
  [project-id dataset]
  (->> (sources project-id dataset)
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
    (let [job (bq/extract-job (bq/service {:project-id (:project-id source-table)}) source-table uri)]
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
  (let [{:keys [job-id]} job]
    (let [job-state        (bq/job (bq/service {:project-id (:project-id job-id)})
                                   job-id)]
      (cond (pending? job-state)       [:pending job-state]
            (bq/running? job-state)    [:running job-state]
            (failed? job-state)        [:failed job-state]
            (bq/successful? job-state) [:successful job-state]))))

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

(defn load-table [{:keys [destination-table source-table extract-uri] :as current-state}]
  (let [table                 (bq/table (bq/service {:project-id (:project-id source-table)}) source-table)
        schema                (get-in table [:definition :schema])]
    (let [job (bq/load-job (bq/service {:project-id (:project-id destination-table)})
                           destination-table
                           {:create-disposition :needed
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


;; sources:
;;   project, dataset, table
;;
;; destinations:
;;   table is input, project and dataset (although maybe overridden from cli)
;;
;; need to find tables to sync, but may have different project/dataset
;; so probably need to diff on just table
;; but then build destination based on source + cli

(defn missing-tables [sources destinations]
  (let [s (map :table-id sources)
        d (map :table-id destinations)
        t (se/difference (set s) (set d))]
    (->> sources
         (filter (fn [{:keys [table-id]}]
                   (some (set [table-id]) t))))))

(defn- override [m k overrides]
  (let [override (k overrides)]
    (update-in m [k] (fn [val] (or override val)))))

(defn destination-table
  [table overrides]
  (-> table
      (override :project-id overrides)
      (override :dataset-id overrides)))

(defn -main [& args]
  (let [{:keys [options summary errors]} (parse-opts args cli)]
    (when errors
      (println errors)
      (System/exit 1))
    (when (:help options)
      (println summary)
      (System/exit 0))
    (let [{:keys [source-project source-dataset
                  destination-project destination-dataset]} options
          overrides      {:project-id destination-project
                          :dataset-id destination-dataset}
          sources        (sessions-sources source-project source-dataset)
          destinations   (->> (sessions-sources destination-project (or destination-dataset
                                                                        source-dataset)))
          target         (missing-tables sources destinations)
          sorted-targets (->> target (sort-by :table-id) (reverse) (take (:number options)))
          in-ch          (a/chan)
          completed-ch   (a/chan)]
      (a/thread
        (info "syncing" (count sorted-targets) "tables:\n" (st/join "\n" (map pr-str sorted-targets)))
        (doseq [t sorted-targets]
          (let [{:keys [google-cloud-bucket]} options
                state {:source-table      t
                       :destination-table (destination-table t overrides)
                       :staging-bucket    google-cloud-bucket
                       :state             :extract}]
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
