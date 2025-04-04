(ns org.zsxf.query-test
  (:require
   #?(:clj [clojure.test :refer [deftest is]])
   #?(:cljs [cljs.test :refer-macros [deftest is]])
   [datascript.core :as d]
   [datascript.db :as ddb]
   [net.cgrand.xforms :as xforms]
   [org.zsxf.datascript :as ds]
   [org.zsxf.query :as q]
   [org.zsxf.util :as util :refer [nth2]]
   [org.zsxf.xf :as xf]
   [org.zsxf.zset :as zs]
   [taoensso.timbre :as timbre]))

; Aggregates current limitation: retractions (deletes) have to be precise!
; An _over-retraction_ by trying to retract a datom that doesn't exist will result in
; an incorrect index state.
; Luckily, DataScript/Datomic correctly report :tx-data without any extra over-deletions.
; Still, it would be more robust to actually safeguard around this.
; Possibly this would require maintaining the entire joined state so attempted
; over-retractions can be filtered out when their delta does not result in a change
; to the joined state

(defn aggregate-example-xf [query-state]
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


;PROBLEM 1:
; once we reach certain clauses, joining to a previous clause can become ambiguous
; when using only numbered (first, second, nth, etc) paths
;
;

(defn >inst [inst-1 inst-2]
  (condp = (compare inst-1 inst-2)
    0 false
    -1 false
    1 true))

(comment
  '[:find ?actor
    :where
    [?danny :person/name "Danny Glover"]
    [?danny :person/born ?danny-born]

    [?a :person/name ?actor]
    [?a :person/born ?actor-born]
    [_ :movie/cast ?a]

    [(> ?danny-born ?actor-born)]])

(defn actors-older-than-danny [query-state]
  (comp
    (xf/mapcat-zset-transaction-xf)
    ;danny
    (xf/join-xf
      ;[?danny :person/born ?danny-born]
      #(ds/datom-attr= % :person/born) ds/datom->eid
      ;[?danny :person/name "Danny Glover"]
      #(ds/datom-attr-val= % :person/name "Danny Glover") ds/datom->eid
      query-state)

    ;actors
    (xf/join-xf
      ;[?a :person/name ?actor]
      #(ds/datom-attr= % :person/name) ds/datom->eid
      ;[?a :person/born ?actor-born]
      #(ds/datom-attr= % :person/born) ds/datom->eid
      query-state)

    (xf/join-right-pred-1-xf
      ;[?a :person/born ?actor-born]
      #(ds/datom-attr= % :person/born) ds/datom->eid
      ;[_ :movie/cast ?a]
      #(ds/datom-attr= % :movie/cast) ds/datom->val
      query-state)

    ;danny joins actors where...
    ;[(> ?danny-born ?actor-born)]
    (xf/join-left-pred-1-xf
      ;?danny-born --> [?danny :person/born ?danny-born]
      #(ds/datom-attr= % :person/born) ds/datom->val
      ;?actor-born
      #(ds/datom-attr= (-> % (nth2 0) (nth2 1)) :person/born) #(-> % (nth2 0) (nth2 1) ds/datom->val)
      query-state
      :return-zset-item-xf
      (comp
        (filter
          (fn [rel1+rel2]
            (timbre/info "reached here...")
            (let [inst-1 (timbre/spy (-> rel1+rel2 first (nth2 0) ds/datom->val))
                  inst-2 (timbre/spy (-> rel1+rel2 second (nth2 0) (nth2 1) (ds/datom->val)))]
              (timbre/info rel1+rel2)
              (timbre/info inst-1)
              (timbre/info inst-2)
              (when (and inst-1 inst-2)
                (>inst
                  ;[?danny :person/born ?danny-born]'s value
                  inst-1
                  ;[?a :person/born ?actor-born]'s value... inside another relation
                  ;notice the (nth2 0) (nth2 1) part of the path is the same as above
                  inst-2))))))
      :last? true)
    (xforms/reduce (zs/zset-xf+
                     (map (xf/with-meta-f
                            (fn [rel1+rel2]
                              (timbre/info rel1+rel2))))))
    (map (fn [final-xf-delta] (timbre/spy final-xf-delta))))
  )

(comment
  ;p2 looking for a joined relation like:
  (let [rel [['[?a :person/name ?actor]
              '[?a :person/born ?actor-born]]
             '[_ :movie/cast ?a]]]
    ;looking for [?a :person/born ?actor-born]
    ; so the path is:
    (-> rel
      (nth2 0)
      (nth2 1)))
  ;=> [?a :person/born ?actor-born]
  )

(defn load-learn-db
  []
  (let [schema (util/read-edn-file "resources/learndatalogtoday/schema_datascript.edn")
        data   (util/read-edn-file "resources/learndatalogtoday/data_datascript.edn")]
    [schema data]))

(comment

  (do
    (timbre/set-min-level! :info)
    (let [[schema data] (load-learn-db)]

      (def conn (d/create-conn schema))
      #_(d/transact! conn
        [{:person/born #inst"2025-01-01T00:00:00.000-00:00"
          :person/name "Danny Glover"}])
      (d/transact! conn data))

    (def query-1 (q/create-query actors-older-than-danny))
    (ds/init-query-with-conn query-1 conn)
    (q/get-result query-1))

  (d/q
    '[:find ?actor (count ?danny)
      :where
      [?danny :person/name "Danny Glover"]
      [?danny :person/born ?danny-born]

      [?a :person/name ?actor]
      [?a :person/born ?actor-born]
      ;[_ :movie/cast ?a]

      [(> ?danny-born ?actor-born)]]
    @conn)

  (d/q
    '[:find ?danny
      :where
      [?danny :person/name "Danny Glover"]]
    @conn)

  conn

  )
