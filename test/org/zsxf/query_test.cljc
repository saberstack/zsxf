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
(defn aggregate-example-xf-join-2 [query-state]
  (comment
    ;equivalent query
    '[:find ?country (sum ?pts)
      :where
      [?e :team/name "A"]
      [?e :event/country ?country]
      [?e :team/points-scored ?pts]])

  (comp
    (xf/mapcat-zset-transaction-xf)
    (xf/join-xf-2
      '[?e :team/name "A"]
      #(ds/datom-attr-val= % :team/name "A") ds/datom->eid
      '[?e :event/country ?country]
      #(ds/datom-attr= % :event/country) ds/datom->eid
      query-state)
    (xf/join-xf-2
      '[?e :event/country ?country]
      #(ds/datom-attr= (-> % (nth2 1)) :event/country) #(-> % (nth2 1) ds/datom->eid)
      '[?e :team/points-scored ?pts]
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
  (def query-1 (q/create-query aggregate-example-xf-join-2))


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

(defn person-city-country-example-xf [query-state]
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
      #(ds/datom-attr-val= % :person/name "Alice") ds/datom->eid
      #(ds/datom-attr= % :person/country) ds/datom->eid
      query-state)
    (xf/join-xf
      #(ds/datom-attr= (-> % (nth2 1)) :person/country) #(-> % (nth2 1) ds/datom->val)
      #(ds/datom-attr-val= % :country/continent "Europe") ds/datom->eid
      query-state)
    (xf/join-xf
      #(ds/datom-attr= (-> % (util/nth2 0) (util/nth2 0)) :person/name) #(-> % (util/nth2 0) (util/nth2 0) ds/datom->eid)
      #(ds/datom-attr-val= % :likes "pizza") ds/datom->eid
      query-state
      :last? true)
    (xforms/reduce zs/zset+)
    (map (fn [final-xf-delta] (timbre/spy final-xf-delta)))))


(comment
  ;subquery explore
  '[:find ?p
    :where
    [?p :person/name "Alice"]
    [?p :person/country ?c]
    [?c :country/continent "Europe"]
    [?p :likes "pizza"]
    [?p2 :person/name "Bob"]
    ;?p2 (Bob) has the same country as ?p (Alice)
    [?p2 :person/country ?c]
    ;and he also must like pizza
    ;this is one fully formed subquery (no breaks in the chain)
    ;but without unique identifiers, are we talking about Bob or Alice here?
    [?p2 :likes "pizza"]]
  )

(defn person-city-country-example-xf-join-2 [query-state]
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
    (xf/join-xf-2
      '[?p :person/name "Alice"]
      #(ds/datom-attr-val= % :person/name "Alice") ds/datom->eid
      '[?p :person/country ?c]
      #(ds/datom-attr= % :person/country) ds/datom->eid
      query-state)
    (xf/join-xf-2
      '[?p :person/country ?c]
      #(ds/datom-attr= (-> % (nth2 1)) :person/country) #(-> % (nth2 1) ds/datom->val)
      '[?c :country/continent "Europe"]
      #(ds/datom-attr-val= % :country/continent "Europe") ds/datom->eid
      query-state)
    (xf/join-xf-2
      ;this works but it's semantically... weird...
      ;ideally we should be able to pass [?p :person/name "Alice"]
      ;right now it only works if we pass [?c :country/continent "Europe"]
      ;i.e. a clause that identifies one of the last two joined relations
      '[?c :country/continent "Europe"]                     ;TODO fix to be [?p :person/name "Alice"]
      #(ds/datom-attr= (-> % (util/nth2 0) (util/nth2 0)) :person/name) #(-> % (util/nth2 0) (util/nth2 0) ds/datom->eid)
      '[?p :likes "pizza"]
      #(ds/datom-attr-val= % :likes "pizza") ds/datom->eid
      query-state
      :last? true)
    (xforms/reduce zs/zset+)
    (map (fn [final-xf-delta] (timbre/spy final-xf-delta)))))

(comment
  ;example usage
  (def query-1 (q/create-query person-city-country-example-xf))
  (def query-1 (q/create-query person-city-country-example-xf-join-2))


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
      ;[?a :person/born ?actor-born]
      ])

  (comp
    (xf/mapcat-zset-transaction-xf)
    ;danny
    ;(xf/join-xf-2
    ;  '[?danny :person/name "Danny Glover"]
    ;  #(ds/datom-attr-val= % :person/name "Danny Glover") ds/datom->eid
    ;  '[?danny :person/born ?danny-born]
    ;  #(ds/datom-attr= % :person/born) ds/datom->eid
    ;  query-state)
    ;;danny, movie cast
    ;(xf/join-xf-2
    ;  '[?danny :person/born ?danny-born]
    ;  #(ds/datom-attr= (-> % (nth2 1)) :person/born) #(-> % (nth2 1) ds/datom->eid)
    ;  '[?m :movie/cast ?danny]
    ;  #(ds/datom-attr= % :movie/cast) ds/datom->val
    ;  query-state)
    ;;movie title
    ;(xf/join-xf-2
    ;  '[?m :movie/cast ?danny]
    ;  #(ds/datom-attr= (-> % (nth2 1)) :movie/cast) #(-> % (nth2 1) ds/datom->eid)
    ;  '[?m :movie/title ?title]
    ;  #(ds/datom-attr= % :movie/title) ds/datom->eid
    ;  query-state)
    ;;actors, movie cast
    ;(xf/join-xf-2
    ;  '[?m :movie/title ?title]
    ;  #(ds/datom-attr= (-> % (nth2 1)) :movie/title) #(-> % (nth2 1) ds/datom->eid)
    ;  '[?m :movie/cast ?a]
    ;  #(ds/datom-attr= % :movie/cast) ds/datom->eid
    ;  query-state
    ;  )
    ;;;actors, :person/name
    ;(xf/join-xf-2
    ;  '[?m :movie/cast ?a]
    ;  #(ds/datom-attr= (-> % (nth2 1)) :movie/cast) #(-> % (nth2 1) ds/datom->val)
    ;  '[?a :person/name ?actor]
    ;  #(ds/datom-attr= % :person/name) ds/datom->eid
    ;  query-state)
    ;;;actors, :person/born
    ;(xf/join-xf-2
    ;  '[?a :person/name ?actor]
    ;  #(ds/datom-attr= (-> % (nth2 1)) :person/name) #(-> % (nth2 1) ds/datom->eid)
    ;  '[?a :person/born ?actor-born]
    ;  #(ds/datom-attr= % :person/born) ds/datom->eid
    ;  query-state
    ;  :last? true)
    (map (fn [pre-reduce] (timbre/spy pre-reduce)))
    (xforms/reduce
      (zs/via-meta-zset-xf+
        (fn [zset-meta]
          (when (some? zset-meta)
            ;(timbre/info "zset-meta found::" zset-meta)
            ;(timbre/info "clauses count::" (count (::xf/clauses zset-meta)))
            )
          (map (xf/with-meta-f
                 (fn [joined-relation]
                   ;(timbre/info "joined-relation::" joined-relation)
                   joined-relation))))))
    (map (fn [final-xf-delta] (timbre/spy final-xf-delta))))
  )

(defn join-xf-with-clauses-test-2
  [query-state]
  (comp
    (xf/mapcat-zset-transaction-xf)
    ;danny
    (xf/join-xf-2
      '[?danny :person/name "Danny Glover"]
      #(ds/datom-attr-val= % :person/name "Danny Glover") ds/datom->eid
      '[?m :movie/cast ?danny]
      #(ds/datom-attr= % :movie/cast) ds/datom->val
      query-state)
    ;movie cast
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    (xf/join-xf-2
      '[?m :movie/cast ?danny]
      #(ds/datom-attr= (-> % (nth2 1)) :movie/cast) #(-> % (nth2 1) ds/datom->eid)
      '[?m :movie/title ?title]
      #(ds/datom-attr= % :movie/title) ds/datom->eid
      query-state)
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    ;actors, movie cast
    (xf/join-xf-2
      '[?m :movie/title ?title]
      #(ds/datom-attr= (-> % (nth2 1)) :movie/title) #(-> % (nth2 1) ds/datom->eid)
      '[?m :movie/cast ?a]
      #(ds/datom-attr= % :movie/cast) ds/datom->eid
      query-state)
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    (xf/join-xf-2
      '[?m :movie/cast ?a]
      #(ds/datom-attr= (-> % (nth2 1)) :movie/cast) #(-> % (nth2 1) ds/datom->val)
      '[?a :person/name ?actor]
      #(ds/datom-attr= % :person/name) ds/datom->eid
      query-state
      :last? true)
    (map (fn [zset-in-between-last] (timbre/spy zset-in-between-last)))
    (xforms/reduce
      (zs/via-meta-zset-xf+
        (fn [zset-meta]
          (timbre/spy zset-meta)
          (map (xf/with-meta-f
                 (fn [joined-relation]
                   joined-relation))))))
    (map (fn [final-xf-delta] (timbre/spy final-xf-delta)))))

(comment
  (do
    (timbre/set-min-level! :trace)
    (timbre/set-min-level! :info)
    (let [[schema data] (load-learn-db)]

      (def conn (d/create-conn schema))
      #_(d/transact! conn
        [{:person/born #inst"2025-01-01T00:00:00.000-00:00"
          :person/name "Danny Glover"}])
      ;(d/transact! conn
      ;  [{:person/born #inst"1900"
      ;    :person/name "Arnold"}])
      (d/transact! conn data)
      )
    (def query-1 (q/create-query join-xf-with-clauses-test-2))
    (ds/init-query-with-conn query-1 conn)
    (count (q/get-result query-1))
    )

  (d/transact! conn
    [{:person/born #inst"2000"}])

  (d/transact! conn
    [{:db/id 1 :person/name "Danny Glover"}])
  (d/transact! conn
    [{:db/id 2 :movie/title "Movie-1"}])

  (q/get-state query-1)

  (d/transact! conn
    [{:db/id 2 :movie/cast 1}])



  (d/transact! conn
    [{:db/id -1 :person/name "Alice"}
     {:db/id 2 :movie/cast -1}])

  (d/transact! conn
    [{:db/id 2 :movie/cast 3}])


  (d/transact! conn
    [{:db/id       -1
      :movie/title "Untitled"}
     {:db/id      -1
      :movie/cast 1}
     {:db/id      -1
      :movie/cast 2}])

  (q/get-result query-1)
  (q/get-state query-1)


  (d/q
    '[:find ?danny ?actor
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

  (d/q
    '[:find ?m ?a
      :where
      [?danny :person/name "Danny Glover"]
      [?danny :person/born ?danny-born]
      [?m :movie/cast ?danny]
      [?m :movie/title ?title]
      [?m :movie/cast ?a]
      [?a :person/name ?actor]
      [(not= ?actor "Danny Glover")]
      ;[?a :person/born ?actor-born]
      ]
    @conn)

  (d/q
    '[:find ?danny
       :where
       [?danny :person/name "Danny Glover"]
       [?danny :person/born ?danny-born]
       [?m :movie/cast ?danny]
       [?m :movie/title ?title]
       [?m :movie/cast ?a]]
    @conn)

  conn

  )
