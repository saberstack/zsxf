(ns org.zsxf.query-test
  (:require [clojure.test :refer :all]
            [datascript.db :as ddb]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.datascript :as ds]
            [org.zsxf.query :as q]
            [org.zsxf.util :as util]
            [org.zsxf.xf :as xf]
            [org.zsxf.zset :as zs]
            [taoensso.timbre :as timbre]))


(defn aggregate-example-xf [query-state]

  ; Aggregates current limitation: retractions (deletes) have to be precise!
  ; An _over-retraction_ by trying to retract a datom that doesn't exist will result in
  ; an incorrect index state.
  ; Luckily, DataScript/Datomic correctly report :tx-data without any extra over-deletions.
  ; Still, it would be more robust to actually safeguard around this.
  ; Possibly this would require maintaining the entire joined state so attempted
  ; over-retractions can be filtered out when their delta does not result in a change
  ; to the joined stat

  (comment
    ;equivalent query
    '[:find ?country (sum ?pts)
      :where
      [?e :team/name "A"]
      [?e :event/country ?country]
      [?e :team/points-scored ?pts]])

  (comp
    (xf/mapcat-zset-transaction-xf)
    (xf/join-xf
      #(ds/datom-attr-val= % :team/name "A") ds/datom->eid
      #(ds/datom-attr= % :event/country) ds/datom->eid
      query-state)
    (xf/join-right-pred-1-xf
      #(ds/datom-attr= % :event/country) ds/datom->eid
      #(ds/datom-attr= % :team/points-scored) ds/datom->eid
      query-state
      :last? true)
    (xforms/reduce zs/zset+)
    ;group by aggregates
    (xf/group-by-xf
      #(-> % (util/nth2 0) (util/nth2 1) ds/datom->val)
      (comp
        (xforms/transjuxt {:sum (xforms/reduce
                                  (zs/zset-sum+
                                    #(-> % (util/nth2 1) ds/datom->val)))
                           :cnt (xforms/reduce zs/zset-count+)})
        (mapcat (fn [{:keys [sum cnt]}]
                  [(zs/zset-sum-item sum)
                   (zs/zset-count-item cnt)]))))
    (map (fn [final-xf-delta] (timbre/spy final-xf-delta)))))

(comment
  ;example usage
  (def query-1 (q/create-query aggregate-example-xf))

  (q/input query-1
    [(ds/tx-datoms->zset
       [[1 :team/name "A" 536870913 true]
        [1 :event/country "Japan" 536870913 true]
        [1 :team/points-scored 25 536870913 true]
        [2 :team/name "A" 536870913 true]
        [2 :event/country "Japan" 536870913 true]
        [2 :team/points-scored 18 536870913 true]
        [3 :team/name "A" 536870913 true]
        [3 :event/country "Australia" 536870913 true]
        [3 :team/points-scored 25 536870913 true]
        [4 :team/name "A" 536870913 true]
        [4 :event/country "Australia" 536870913 true]
        [4 :team/points-scored 4 536870913 true]])])

  (q/get-result query-1)

  (q/input query-1
    [(ds/tx-datoms->zset
       [[1 :team/name "A" 536870913 false]
        [2 :team/name "A" 536870913 false]
        [3 :team/name "A" 536870913 false]])])

  (q/get-result query-1)

  )

(comment
  ;example usage
  (def query-1 (q/create-query aggregate-example-xf))


  (q/input query-1
    [(ds/tx-datoms->datoms2->zset
       [(ddb/datom 1 :team/name "A" 536870913 true)
        (ddb/datom 1 :event/country "Japan" 536870913 true)
        (ddb/datom 1 :team/points-scored 25 536870913 true)
        (ddb/datom 2 :team/name "A" 536870913 true)
        (ddb/datom 2 :event/country "Japan" 536870913 true)
        (ddb/datom 2 :team/points-scored 18 536870913 true)
        (ddb/datom 3 :team/name "A" 536870913 true)
        (ddb/datom 3 :event/country "Australia" 536870913 true)
        (ddb/datom 3 :team/points-scored 25 536870913 true)
        (ddb/datom 4 :team/name "A" 536870913 true)
        (ddb/datom 4 :event/country "Australia" 536870913 true)
        (ddb/datom 4 :team/points-scored 4 536870913 true)])])

  (q/get-result query-1)

  (q/input query-1
    [(ds/tx-datoms->datoms2->zset
       [(ddb/datom 1 :team/name "A" 536870913 false)
        (ddb/datom 2 :team/name "A" 536870913 false)
        (ddb/datom 3 :team/name "A" 536870913 false)])])

  (q/get-result query-1)

  (q/get-state query-1)

  )
