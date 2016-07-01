(ns uswitch.big-replicate.materialize
  (:require [clojure.tools.cli :refer (parse-opts)]
            [clojure.tools.logging :refer (info error)]
            [gclouj.bigquery :as bq])
  (:gen-class))

(def cli [["-p" "--project-id PROJECT_ID" "Google Cloud Project ID"]
          ["-d" "--dataset-id DATASET_ID" "Output Dataset ID"]
          ["-t" "--table-id TABLE_ID"     "Output Table ID"]
          ["-f" "--force"                 "Overwrite destination table contents" :default false]
          ["-h" "--help"]])

(defn waiting? [job]
  (let [state (get-in job [:status :state])]
    (or (= :running state)
        (= :pending state))))

(def finished? (complement waiting?))

(defn wait-for-job! [bigquery job-id]
  (loop [job (bq/job bigquery job-id)]
    (if (finished? job)
      job
      (do (info "waiting for job:" job)
          (Thread/sleep 10000)
          (recur (bq/job bigquery job-id))))))

(defn errors [job]
  (get-in job [:status :errors]))

(defn materialize [query {:keys [project-id dataset-id table-id force]}]
  (let [bigquery    (bq/service)
        destination {:project-id project-id
                     :dataset-id dataset-id
                     :table-id   table-id}]
    (let [{:keys [job-id] :as job} (bq/query-job bigquery query {:create-disposition :needed
                                                                 :write-disposition  (if force :truncate :empty)
                                                                 :large-results?     true
                                                                 :destination-table  destination
                                                                 :use-cache?         false
                                                                 :priority           :interactive})]
      (info "started materialize job" job)
      (let [job (wait-for-job! bigquery job-id)
            es  (errors job)]
        (if (seq es)
          (do (error "failed:" es)
              (System/exit 1))
          (info "completed" job))))))

(defn -main [& args]
  (let [{:keys [options summary errors]} (parse-opts args cli)]
    (when errors
      (println errors)
      (System/exit 1))
    (when (:help options)
      (println summary)
      (System/exit 0))
    (materialize (slurp *in*) options)))
