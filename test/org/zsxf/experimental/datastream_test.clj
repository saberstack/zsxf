(ns org.zsxf.experimental.datastream-test
  (:require
   [clojure.test :refer [deftest testing is are]]
   [datascript.core :as d]
   [org.zsxf.zset :as zset]
   [org.zsxf.test-data.states :as states-data]
   [org.zsxf.experimental.datastream :as subject]))

(def empty-db (partial d/create-conn states-data/schema))

(defn- state-names [zset-of-datoms]
  (->> zset-of-datoms
       (filter (fn [[_ a _]] (= a :state/name) ))
       (map (fn [[_ _ v]] v))
       set))

(deftest populate-existing-db
  (testing "A single transaction comes through as a single zset"
    (let [test-db (d/db-with @(empty-db) (take 3 states-data/statoms))
          changes (subject/db->stream-of-changes test-db)
          first-zset (first changes)]
      (is (= 1 (count changes)))
      (is (= #{"Alabama" "Alaska" "Arizona"}
             (state-names first-zset)))))
  (testing "Multiple transactions come through as multiple zsets"
    (let [test-db (reduce
                   d/db-with
                   @(empty-db)
                   (->> states-data/statoms
                        (take 7)
                        (partition-all 3)))
          changes (subject/db->stream-of-changes test-db)]
      (is (= [#{"Alabama" "Alaska" "Arizona"}
              #{"Arkansas" "California" "Colorado"}
              #{"Connecticut"}] (map state-names changes)))
      (testing "Zsets can be combined"
        (is (= #{"Alabama" "Alaska" "Arizona" "Arkansas" "California" "Colorado" "Connecticut"}
               (->> changes (reduce zset/zset+) state-names))))))
  (testing "Retractions don't show up as transactions in themselves"
    (let [test-db (reduce
                    d/db-with
                    @(empty-db)
                    [(take 3 states-data/statoms)
                     [[:db.fn/retractEntity 1]]])
          changes (subject/db->stream-of-changes test-db)]
      (is (= [#{"Alaska" "Arizona"}]
             (map state-names changes))))))
