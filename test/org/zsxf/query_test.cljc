(ns org.zsxf.query-test
  (:require
   #?(:clj [clojure.test :refer [deftest is]])
   #?(:cljs [cljs.test :refer-macros [deftest is]])
   [datascript.core :as d]
   [datascript.db :as ddb]
   [net.cgrand.xforms :as xforms]
   [org.zsxf.datascript :as ds]
   [org.zsxf.query :as q]
   [org.zsxf.datalog.compiler]
   [org.zsxf.util :as util :refer [nth2 path-f]]
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

;TODO use join-xf-3
#_(defn aggregate-example-xf [query-state]
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

;TODO use join-xf-3
#_(comment
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

  (q/get-state query-1))

(comment
  ;subquery explore
  '[:find ?p
    :where
    ;Alice
    [?p :person/name "Alice"]
    [?p :person/country ?c]
    [?c :country/continent "Europe"]
    [?p :likes "pizza"]

    ;Bob
    ;?p2 (Bob) has the same country as ?p (Alice)
    ;and he also must like pizza
    ;this is one fully formed subquery (no breaks in the chain)
    ;but without unique identifiers, are we talking about Bob or Alice here?
    [?p2 :person/name "Bob"]
    [?p2 :person/country ?c]
    [?c :country/continent "Europe"]
    ;with no identifiers/clauses, this is ambiguous::
    [?p2 :likes "pizza"]]
  )

(defn person-city-country-example-xf-join-3 [query-state]
  (comment
    ;equivalent query
    '[:find ?p
      :where
      [?p :person/name "Alice"]
      [?p :person/country ?c]
      [?c :country/continent "Europe"]
      [?p :likes "pizza"]])

  (comp
    (xf/mapcat-zset-transaction-xf)
    (xf/join-xf
      {:clause    '[?p :person/name "Alice"]
       :pred      #(ds/datom-attr-val= % :person/name "Alice")
       :index-kfn ds/datom->eid}
      {:clause    '[?p :person/country ?c]
       :pred      #(ds/datom-attr= % :person/country)
       :index-kfn ds/datom->eid}
      query-state)
    (xf/join-xf
      {:clause    '[?p :person/country ?c]
       :path      (util/path-f [1])
       :pred      #(ds/datom-attr= % :person/country)
       :index-kfn ds/datom->val}
      {:clause    '[?c :country/continent "Europe"]
       :pred      #(ds/datom-attr-val= % :country/continent "Europe")
       :index-kfn ds/datom->eid}
      query-state
      :last? true)
    ;(xf/join-xf-3
    ;  '[?p :person/name "Alice"]
    ;  [(path-f [0 0])
    ;   #(ds/datom-attr= % :person/name)] ds/datom->eid
    ;  '[?p :likes "pizza"]
    ;  [(path-f [])
    ;   #(ds/datom-attr-val= % :likes "pizza")] ds/datom->eid
    ;  query-state
    ;  :last? true)
    (xforms/reduce zs/zset+)
    (map (fn [final-xf-delta] (timbre/spy final-xf-delta)))))



(deftest join-xf-3-test-1
  (let [_      (timbre/set-min-level! :trace)
        query  (q/create-query person-city-country-example-xf-join-3)
        datoms (ds/tx-datoms->zsets2
                 [(ddb/datom 2 :person/country 1 536870913 true)
                  (ddb/datom 2 :person/name "Alice" 536870913 true)
                  (ddb/datom 1 :country/continent "Europe" 536870913 true)])]
    (q/input query datoms)
    (def q query)
    (q/get-result query)))

(comment
  ;example usage
  (def query-1 (q/create-query person-city-country-example-xf-join-3))


  (q/input query-1
    [(ds/tx-datoms->datoms2->zset
       [(ddb/datom 1 :country/continent "Europe" 536870913 true)
        (ddb/datom 2 :person/name "Alice" 536870913 true)
        (ddb/datom 2 :likes "pizza" 536870913 true)
        (ddb/datom 2 :person/country 1 536870913 true)])])

  (q/get-result query-1)

  (q/input query-1
    [(ds/tx-datoms->datoms2->zset
       [(ddb/datom 1 :team/name "A" 536870913 false)
        (ddb/datom 2 :team/name "A" 536870913 false)
        (ddb/datom 3 :team/name "A" 536870913 false)])])

  (q/get-result query-1)

  (q/get-state query-1)

  )

(defn load-learn-db
  []
  (let [schema (util/read-edn-file "resources/learndatalogtoday/schema_datascript.edn")
        data   (util/read-edn-file "resources/learndatalogtoday/data_datascript.edn")]
    [schema data]))

(defn new-join-xf-3
  [query-state]
  (comp
    (xf/mapcat-zset-transaction-xf)
    ;danny
    (xf/join-xf
      {:clause    '[?danny :person/name "Danny Glover"]
       :path      (path-f [])
       :pred      #(ds/datom-attr-val= % :person/name "Danny Glover")
       :index-kfn ds/datom->eid}
      {:clause    '[?m :movie/cast ?danny]
       :path      (path-f [])
       :pred      #(ds/datom-attr= % :movie/cast)
       :index-kfn ds/datom->val}
      query-state)
    ;movie cast
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    (xf/join-xf
      {:clause    '[?m :movie/cast ?danny]
       :path      (path-f [1])
       :pred      #(ds/datom-attr= % :movie/cast)
       :index-kfn ds/datom->eid}
      {:clause    '[?m :movie/title ?title]
       :pred      #(ds/datom-attr= % :movie/title)
       :index-kfn ds/datom->eid}
      query-state)
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    ;actors, movie cast
    (xf/join-xf
      {:clause    '[?m :movie/title ?title]
       :path      (path-f [1])
       :pred      #(ds/datom-attr= % :movie/title)
       :index-kfn ds/datom->eid}
      {:clause    '[?m :movie/cast ?a]
       :pred      #(ds/datom-attr= % :movie/cast)
       :index-kfn ds/datom->eid}
      query-state)
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    (xf/join-xf
      {:clause    '[?m :movie/cast ?a]
       :path      (path-f [1])
       :pred      #(ds/datom-attr= % :movie/cast)
       :index-kfn ds/datom->val}
      {:clause    '[?a :person/name ?actor]
       :pred      #(ds/datom-attr= % :person/name)
       :index-kfn ds/datom->eid}
      query-state
      :last? true)
    (map (fn [zset-in-between-last] (timbre/spy zset-in-between-last)))
    (xforms/reduce zs/zset+)
    (map (fn [final-xf-delta] (timbre/spy final-xf-delta)))))

(def datalog-query-1
  '[:find ?danny ?m ?title ?m ?a
    :where
    [?danny :person/name "Danny Glover"]
    [?danny :person/born ?danny-born]
    [?m :movie/cast ?danny]
    [?m :movie/title ?title]
    [?m :movie/cast ?a]
    [?a :person/name ?actor]
    ])

(deftest join-xf-3-with-another-fix
  (let [_       (timbre/set-min-level! :info)
        [schema data] (load-learn-db)
        conn    (d/create-conn schema)
        _       (d/transact! conn data)
        query-1 (q/create-query new-join-xf-3)
        _       (ds/init-query-with-conn query-1 conn)
        result  (q/get-result query-1)]
    (is (=
          (count (d/q datalog-query-1 @conn))
          (count result)))))
