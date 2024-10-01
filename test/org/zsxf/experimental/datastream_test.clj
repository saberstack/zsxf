(ns org.zsxf.experimental.datastream-test
  (:require
   [clojure.test :refer [deftest testing is are]]
   [datascript.core :as d]
   [org.zsxf.test-data.states :as states-data]
   [org.zsxf.experimental.datastream :as subject]))

(def empty-db (partial d/create-conn states-data/schema))

(deftest populate-existing-db
  (testing "A single transaction comes through as a single zset"
    (let [{test-db :db-after} (d/transact! (empty-db) (take 3 states-data/statoms))
          changes (subject/db->stream-of-changes test-db)
          first-zset (first changes)]
      (is (= 1 (count changes)))
      (is (= #{"Alaska" "Alabama" "Arizona"}
             (->> first-zset
                  (filter (fn [[_ a _]] (= a :state/name) ))
                  (map (fn [[_ _ v]] v))
                  set))))))


#_(deftest db->stream-of-changes-test
  (is true))
