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



(defn >inst [inst-1 inst-2]
  (condp = (compare inst-1 inst-2)
    0 false
    -1 false
    1 true))



;
;Glossary
; - Clause: defines a single relation
; - Joined relation: a pair of relations with arbitrary depth of joined relations nested inside
;    [:R1 :R2] ;two relations (smallest possible joined relation pair)
;    [[:R1 :R2] :R3] ;three relations
;    [[[:R1 :R2] :R3] :R4] ;four relations
;
;
;_Every_ join-xf:
; - takes zset-items & joined relations (from previous join-xfs outputs)
;    (!) Note: zset-items can be datoms but critically can also
;       can be joined relations (a vector pair):
;       [:R1 :R2]
;       [[:R1 :R2] :R3]
;       [[[:R1 :R2] :R3] :R4]
;       Notice the top level vector count is always two (i.e. it's a pair)
;       with more pairs potentially nested at every level
; - outputs joined relations based on predicates and index kfns
; - outputs zset-items, unchanged (until :last?)
;
;PROBLEM 1:
; once we reach a certain :where clause, joining to a previous relation can become ambiguous
; when using only numbered (first, second, nth, etc) paths
;
;



(comment
  ;1. tx, datom shows up
  [1 :person/born 1955]
  ;waiting
  ;2. tx, another datom shows up
  [1 :person/name "Danny Glover"]
  ;3. tx
  [50 :movie/cast 1]

  #{(zs/zset-item [[1 :person/name "Danny Glover"]
                   [1 :person/born 1955]])}
  #{(zs/zset-item [1 :person/name "Danny Glover"])}
  )

(comment
  ;movie cast xf receives this
  #{(zs/zset-item [[1 :person/name "Danny Glover"]
                   [1 :person/born 1955]])}

  #{(zs/zset-item [[1 :person/name "Danny Glover"]
                   [1 :person/born 1955]])}
  ;original datom
  #{(zs/zset-item [1 :person/name "Danny Glover"])}
  )
(comment
  ;query
  '[:find ?actor
    :where
    ;danny
    [?danny :person/name "Danny Glover"]
    [?danny :person/born ?danny-born]

    ;actors
    [?a :person/name ?actor]
    [?a :person/born ?actor-born]
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    [_ :movie/cast ?a]
    ;;joins them together
    ;[(> ?danny-born ?actor-born)]
    ])

(defn join-xf-with-clauses-test-1
  [query-state]
  (comment
    '[:find ?danny ?actor ?title
      :where
      [?danny :person/name "Danny Glover"]
      [?danny :person/born ?danny-born]
      [?m :movie/cast ?danny]
      [?m :movie/title ?title]
      [?m :movie/cast ?a]
      [?a :person/name ?actor]
      [?a :person/born ?actor-born]
      ])

  (comp
    (xf/mapcat-zset-transaction-xf)
    ;danny
    (xf/join-xf-2
      '[?danny :person/name "Danny Glover"]
      #(ds/datom-attr-val= % :person/name "Danny Glover") ds/datom->eid
      '[?danny :person/born ?danny-born]
      #(ds/datom-attr= % :person/born) ds/datom->eid
      query-state)
    ;danny, movie cast
    (xf/join-xf-2
      '[?danny :person/born ?danny-born]
      #(ds/datom-attr= (-> % (nth2 1)) :person/born) #(-> % (nth2 1) ds/datom->eid)
      '[?m :movie/cast ?danny]
      #(ds/datom-attr= % :movie/cast) ds/datom->val
      query-state)
    ;;movie title
    (xf/join-xf-2
      '[?m :movie/cast ?danny]
      #(ds/datom-attr= (-> % (nth2 1)) :movie/cast) #(-> % (nth2 1) ds/datom->eid)
      '[?m :movie/title ?title]
      #(ds/datom-attr= % :movie/title) ds/datom->eid
      query-state)
    ;actors, movie cast
    (xf/join-xf-2
      '[?m :movie/title ?title]
      #(ds/datom-attr= (-> % (nth2 1)) :movie/title) #(-> % (nth2 1) ds/datom->eid)
      '[?m :movie/cast ?a]
      #(ds/datom-attr= % :movie/cast) ds/datom->eid
      query-state)
    ;;actors, :person/name
    (xf/join-xf-2
      '[?m :movie/cast ?a]
      #(ds/datom-attr= (-> % (nth2 1)) :movie/cast) #(-> % (nth2 1) ds/datom->val)
      '[?a :person/name ?actor]
      #(ds/datom-attr= % :person/name) ds/datom->eid
      query-state)
    ;;actors, :person/born
    (xf/join-xf-2
      '[?a :person/name ?actor]
      #(ds/datom-attr= (-> % (nth2 1)) :person/name) #(-> % (nth2 1) ds/datom->eid)
      '[?a :person/born ?actor-born]
      #(ds/datom-attr= % :person/born) ds/datom->eid
      query-state
      :last? true)
    (map (fn [pre-reduce] (timbre/spy pre-reduce)))
    (xforms/reduce
      #_zs/zset+
      (zs/zset-xf+
        (map (xf/with-meta-f
               (fn [joined-relation]
                 ;TODO :find
                 (timbre/spy (meta joined-relation))
                 joined-relation)))))
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
    (timbre/set-min-level! :trace)
    (let [[schema data] (load-learn-db)]

      (def conn (d/create-conn schema))
      ;(d/transact! conn
      ;  [{:person/born #inst"2025-01-01T00:00:00.000-00:00"
      ;    :person/name "Danny Glover"}])
      ;(d/transact! conn
      ;  [{:person/born #inst"1900"
      ;    :person/name "Arnold"}])
      (d/transact! conn data)
      )

    (def query-1 (q/create-query join-xf-with-clauses-test-1))
    (ds/init-query-with-conn query-1 conn)
    )

  (d/transact! conn
    [{:movie/cast  1
      :movie/title "Untitled"}])

  (d/transact! conn
    [{:db/id       -1
      :movie/title "Untitled"}
     {:db/id      -1
      :movie/cast 1}
     {:db/id      -1
      :movie/cast 2}])

  (q/get-result query-1)

  (comment
    '[:find ?danny ?actor ?title
      :where
      [?danny :person/name "Danny Glover"]
      [?danny :person/born ?danny-born]
      [?m :movie/cast ?danny]
      [?m :movie/title ?title]
      [?m :movie/cast ?a]
      [?a :person/name ?actor]
      ])

  (d/q
    '[:find ?actor
      :where
      [?danny :person/name "Danny Glover"]
      [?danny :person/born ?danny-born]
      [?m :movie/cast ?danny]
      [?m :movie/title ?title]
      [?m :movie/cast ?a]
      [?a :person/name ?actor]
      ]
    @conn)

  ;find can be a join also
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ; no join
  (d/q
    '[:find ?danny
      :where
      ;danny
      [?danny :person/name "Danny Glover"]
      [?danny :person/born ?danny-born]

      ;actors
      [?a :person/name ?actor]
      [?a :person/born ?actor-born]
      [_ :movie/cast ?a]
      ]
    @conn)
  ; join
  (d/q
    '[:find ?danny ?a
      :where
      [?danny :person/name "Danny Glover"]
      [?danny :person/born ?danny-born]

      ;actors
      [?a :person/name ?actor]
      [?a :person/born ?actor-born]
      [_ :movie/cast ?a]
      ]
    @conn)
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;




  (time
    (d/q
      '[:find (count ?danny)
        :where
        [?danny :person/name "Danny Glover"]]
      @conn))

  conn

  )
