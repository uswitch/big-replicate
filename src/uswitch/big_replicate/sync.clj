(ns uswitch.big-replicate.sync
  (:require [gclouj.bigquery :as bq]
            [gclouj.storage :as cs]
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
          ["-f" "--table-filter REGEXP"            "Only tables matching this regexp will be processed"
           :default ".*" :parse-fn re-pattern]
          ["-g" "--google-cloud-bucket BUCKET"     "Staging bucket to store exported data"]
          ["-n" "--number NUMBER"                  "Number of days to look back for missing tables"
           :default 7 :parse-fn #(Integer/parseInt %)]
          ["-a" "--number-of-agents NUMBER"        "Number of concurrent replication agents to run."
           :default (.availableProcessors (Runtime/getRuntime)) :parse-fn #(Integer/parseInt %)]
          ["-h" "--help"                           "Display summary"]])

(defrecord TableReference [project-id dataset-id table-id])

(defn table-ref [{:keys [project-id dataset-id table-id]}]
  (TableReference. project-id dataset-id table-id))

(defn tables
  "Finds all tables in dataset to replicate"
  [project-id dataset pattern]
  (let [service (bq/service {:project-id project-id})]
    (->> (bq/tables service {:project-id project-id
                             :dataset-id dataset})
         (map :table-id)
         (filter (fn [table] (re-matches pattern (:table-id table))))
         (map table-ref))))

(defn staging-location [bucket {:keys [dataset-id table-id] :as table-reference}]
  (let [prefix (format "%s/%s" dataset-id table-id)]
    {:uri    (format "%s/%s/*" bucket prefix)
     :prefix prefix}))




(defn extract-table [{:keys [source-table staging-bucket] :as current-state}]
  {:pre [(.startsWith staging-bucket "gs://")
         (not (.endsWith staging-bucket "/"))]}
  (let [{:keys [uri prefix]} (staging-location staging-bucket source-table)]
    (info "starting extract for" source-table "into" uri)
    (let [job (bq/extract-job (bq/service {:project-id (:project-id source-table)}) source-table uri)]
      (assoc current-state
        :state          :wait-for-extract
        :extract-uri    uri
        :staging-prefix prefix
        :job            job))))

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
(def wait-for-load    (partial wait-for-job :cleanup))

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

(defn cleanup [{:keys [extract-uri staging-bucket staging-prefix] :as current-state}]
  (let [bucket  (st/replace staging-bucket "gs://" "")
        storage (cs/service)]
    (info "deleting staging location" extract-uri)
    (debug "finding objects in" bucket "with prefix" staging-prefix)
    (loop [objects (cs/blobs storage bucket staging-prefix)]
      (if-let [blobs (seq objects)]
        (let [blob (first blobs)]
          (debug "deleting" (:id blob))
          (let [deleted? (cs/delete-blob storage (:id blob))]
            (if deleted?
              (recur (rest objects))
              (assoc current-state
                     :state :failed
                     :cause (format "couldn't delete %s" (pr-str (:id blob)))))))
        (assoc current-state :state :completed)))))

(defn progress [current-state]
  (loop [current-state current-state]
    (let [state (:state current-state)]
      (if-let [op ({:extract          extract-table
                    :wait-for-extract wait-for-extract
                    :load             load-table
                    :wait-for-load    wait-for-load
                    :cleanup          cleanup} state)]
        (recur (try (op current-state)
                    (catch Exception ex
                      (assoc current-state :state :failed :exception ex))))
        current-state))))

(defn replicator-agent [in-ch completed-ch]
  (a/thread
    (loop [state (a/<!! in-ch)]
      (when state
        (let [final-state (progress state)]
          (a/>!! completed-ch final-state))
        (recur (a/<!! in-ch))))))


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

(defn target-tables [{:keys [source-project source-dataset destination-project destination-dataset table-filter number] :as options}]
  (let [sources      (tables source-project source-dataset table-filter)
        destinations (tables destination-project
                             (or destination-dataset
                                 source-dataset)
                             table-filter)]
    (->> (missing-tables sources destinations)
         (sort-by :table-id)
         (reverse)
         (take number))))

(defn -main [& args]
  (let [{:keys [options summary errors]} (parse-opts args cli)]
    (when errors
      (println errors)
      (System/exit 1))
    (when (:help options)
      (println summary)
      (System/exit 0))
    (let [targets (target-tables options)]
      (when (empty? targets)
        (info "no tables to copy")
        (System/exit 0))
      (let [in-ch        (a/chan (:number options))
            completed-ch (a/chan)]
        (info "syncing" (count targets) "tables:\n" (st/join "\n" (map pr-str targets)))
        (let [{:keys [google-cloud-bucket destination-project destination-dataset]} options
              overrides {:project-id destination-project
                         :dataset-id destination-dataset}]
          (->> targets
               (map (fn [source-table]
                      {:source-table      source-table
                       :destination-table (destination-table source-table overrides)
                       :staging-bucket    google-cloud-bucket
                       :state             :extract}))
               (a/onto-chan in-ch)))
        (let [agents (:number-of-agents options)]
          (info "creating" agents "replicator agents")
          (dotimes [_ agents]
            (replicator-agent in-ch completed-ch)))
        (let [expected-count (count targets)]
          (loop [n 1
                 m (a/<!! completed-ch)]
            (when m
              (info (format "%d/%d" n expected-count) "completed")
              (if (= :failed (:state m))
                (error "sync failed. final state:" m)
                (info "sync successful:" (select-keys m [:source-table :destination-table])))
              (if (= n expected-count)
                (info "finished")
                (recur (inc n) (a/<!! completed-ch))))))))))
