(ns org.zsxf.experimental.datastream-test
  (:require
   [clojure.test :refer [deftest testing is are]]
   [datascript.core :as d]
   [org.zsxf.zset :as zset]
   [org.zsxf.test-data.states :as states-data]
   [org.zsxf.experimental.datastream :as subject]))

(def empty-db-conn (partial d/create-conn states-data/schema))

(defn- state-names [zset-of-datoms]
  (->> zset-of-datoms
       (filter (fn [[_ a _]] (= a :state/name) ))
       (map (fn [[_ _ v]] v))
       set))

(deftest populate-existing-db
  (testing "A single transaction comes through as a single zset"
    (let [test-db (d/db-with @(empty-db-conn) (take 3 states-data/statoms))
          changes (subject/db->stream-of-changes test-db)
          first-zset (first changes)]
      (is (= 1 (count changes)))
      (is (= #{"Alabama" "Alaska" "Arizona"}
             (state-names first-zset)))))
  (testing "Multiple transactions come through as multiple zsets"
    (let [test-db (reduce
                   d/db-with
                   @(empty-db-conn)
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
                    @(empty-db-conn)
                    [(take 3 states-data/statoms)
                     [[:db.fn/retractEntity 1]]])
          changes (subject/db->stream-of-changes test-db)]
      (is (= [#{"Alaska" "Arizona"}]
             (map state-names changes))))))

(deftest test-listening
  (let [results (atom [])
        db-conn (empty-db-conn)]
    (do (subject/listen-datom-stream db-conn results)
        (d/transact! db-conn (take 3 states-data/statoms))
        (d/transact! db-conn [[:db.fn/retractEntity 1]]))
    (testing "Each transaction comes as a zset, including retractions."
      (are [x f] (= x (map f @results))
        (testing "Two transactions; Alabama twice."
          [#{"Alabama" "Alaska" "Arizona"} #{"Alabama"}]) state-names
        (testing "The second transaction has negative weights."
          [#{1} #{-1}]) #(set (map zset/zset-weight %))))
    (testing "Subsequent retractions cancel earlier additions."
      (let [combined-zset (reduce zset/zset+ @results)]
        (is (= #{"Alaska" "Arizona"} (state-names combined-zset)))
        (is (= #{1} (set (map zset/zset-weight combined-zset))))))
    (testing "Building a new immutable db from the original one will evade listening."
      (reset! results [])
      (let [new-db (d/db-with @db-conn (take 1 states-data/statoms))]
        (is (= 0 (count @results)))
        (is (= [#{"Alaska" "Arizona"} #{"Alabama"}]
               (map state-names (subject/db->stream-of-changes new-db))))))
    (testing "But subsequent transactions on the original connection will be heard."
      (d/transact! db-conn (->> states-data/statoms (drop 3) (take 1)) )
      (is (= 1 (count @results)))
      (is (= [#{"Alaska" "Arizona"} #{"Arkansas"}]
             (map state-names (subject/db->stream-of-changes @db-conn)))))))
