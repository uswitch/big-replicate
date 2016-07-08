(ns uswitch.big-replicate.sync-test
  (:require [uswitch.big-replicate.sync :refer :all]
            [clojure.test :refer :all]))

(defn- table [project dataset table]
  (map->TableReference {:project-id project
                        :dataset-id dataset
                        :table-id   table}))

(deftest identify-target-tables
  (let [sources [(table "source-project" "source-dataset" "source-table1")]]
    (is (= 1 (count (missing-tables sources []))))
    (is (= (table "source-project" "source-dataset" "source-table1")
           (first (missing-tables sources []))))))

(deftest override-destination
  (is (= (table "source-project" "new-dataset" "table1")
         (destination-table (table "source-project"
                                   "source-destination"
                                   "table1")
                            {:dataset-id "new-dataset"}))))
