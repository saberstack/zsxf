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

(defmacro path-f [v]
  (transduce
    (map (fn [idx#]
           `(fn [x#] (~`util/nth2 x# ~idx#))))
    (completing
      conj
      (fn [accum]
        (conj accum `comp)))
    '()
    v))

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
    (xf/join-xf-3
      {:clause    '[?p :person/name "Alice"]
       :pred      #(ds/datom-attr-val= % :person/name "Alice")
       :index-kfn ds/datom->eid}
      {:clause    '[?p :person/country ?c]
       :pred      #(ds/datom-attr= % :person/country)
       :index-kfn ds/datom->eid}
      query-state)
    (xf/join-xf-3
      {:clause    '[?p :person/country ?c]
       :path      (path-f [1])
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
        datoms [(ds/tx-datoms->datoms2->zset
                  [(ddb/datom 1 :country/continent "Europe" 536870913 true)
                   (ddb/datom 2 :person/name "Alice" 536870913 true)
                   (ddb/datom 2 :likes "pizza" 536870913 true)
                   (ddb/datom 2 :person/country 1 536870913 true)])]
        datoms (ds/tx-datoms->zsets2
                 [(ddb/datom 2 :person/country 1 536870913 true)
                  (ddb/datom 2 :person/name "Alice" 536870913 true)
                  (ddb/datom 1 :country/continent "Europe" 536870913 true)])]
    (q/input query datoms)
    (def q query)
    (q/get-result query)))

(comment
  ;example usage
  (def query-1 (q/create-query person-city-country-example-xf))
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

(defn old-join-xf-without-clauses
  [query-state]
  (comp
    (xf/mapcat-zset-transaction-xf)
    ;danny
    (xf/join-xf
      #(ds/datom-attr-val= % :person/name "Danny Glover") ds/datom->eid
      #(ds/datom-attr= % :movie/cast) ds/datom->val
      query-state)
    ;movie cast
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    (xf/join-xf
      #(ds/datom-attr= (-> % (nth2 1)) :movie/cast) #(-> % (nth2 1) ds/datom->eid)
      #(ds/datom-attr= % :movie/title) ds/datom->eid
      query-state)
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    ;actors, movie cast
    (xf/join-xf
      #(ds/datom-attr= (-> % (nth2 1)) :movie/title) #(-> % (nth2 1) ds/datom->eid)
      #(ds/datom-attr= % :movie/cast) ds/datom->eid
      query-state)
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    (xf/join-xf
      #(ds/datom-attr= (-> % (nth2 1)) :movie/cast) #(-> % (nth2 1) ds/datom->val)
      #(ds/datom-attr= % :person/name) ds/datom->eid
      query-state
      :last? true)
    (map (fn [zset-in-between-last] (timbre/spy zset-in-between-last)))
    (xforms/reduce zs/zset+)
    (map (fn [final-xf-delta] (timbre/spy final-xf-delta)))))

(defn new-join-xf-with-clauses-test-2
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
    (xforms/reduce zs/zset+)
    (map (fn [final-xf-delta] (timbre/spy final-xf-delta)))))

(defn new-join-xf-3
  [query-state]
  (comp
    (xf/mapcat-zset-transaction-xf)
    ;danny
    (xf/join-xf-3
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
    (xf/join-xf-3
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
    (xf/join-xf-3
      {:clause    '[?m :movie/title ?title]
       :path      (path-f [1])
       :pred      #(ds/datom-attr= % :movie/title)
       :index-kfn ds/datom->eid}
      {:clause    '[?m :movie/cast ?a]
       :pred      #(ds/datom-attr= % :movie/cast)
       :index-kfn ds/datom->eid}
      query-state)
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    (xf/join-xf-3
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
        query-1 (q/create-query new-join-xf-3)]

    (ds/init-query-with-conn query-1 conn)
    (is (=
          (count (d/q datalog-query-1 @conn))
          (count (q/get-result query-1))))))

(deftest join-xf-2-with-fix
  (let [_       (timbre/set-min-level! :info)
        [schema data] (load-learn-db)
        conn    (d/create-conn schema)
        _       (d/transact! conn data)
        query-1 (q/create-query new-join-xf-with-clauses-test-2)]

    (ds/init-query-with-conn query-1 conn)
    (is (=
          (count (d/q datalog-query-1 @conn))
          (count (q/get-result query-1))))))

(deftest join-xf-old-problem
  (let [_       (timbre/set-min-level! :info)
        [schema data] (load-learn-db)
        conn    (d/create-conn schema)
        _       (d/transact! conn data)
        query-1 (q/create-query old-join-xf-without-clauses)]

    (ds/init-query-with-conn query-1 conn)
    (is (=
          (count (d/q datalog-query-1 @conn))
          (count (q/get-result query-1))))))

(deftest join-xf-compiler
  (let [_       (timbre/set-min-level! :info)
        [schema data] (load-learn-db)
        conn    (d/create-conn schema)
        _       (d/transact! conn data)
        query-1 (q/create-query compiler-join-xf-1)]

    (ds/init-query-with-conn query-1 conn)
    (=
      (count (d/q datalog-query-1 @conn))
      (count (q/get-result query-1)))
    (q/get-result query-1)))

(comment
  ;old wrong output, 16 items, badly formed
  #{[[[[[14 :person/name "Danny Glover"] [58 :movie/cast 14]] [58 :movie/title "Lethal Weapon 2"]] [58 :movie/cast 37]]
     [37 :person/name "Joe Pesci"]]

    [[[[[14 :person/name "Danny Glover"] [64 :movie/cast 14]] [64 :movie/title "Lethal Weapon 3"]] [64 :movie/cast 14]]
     [14 :person/name "Danny Glover"]]

    [[[[[14 :person/name "Danny Glover"] [64 :movie/cast 14]] [64 :movie/title "Lethal Weapon 3"]] [64 :movie/cast 13]]
     [13 :person/name "Mel Gibson"]]

    [[[[[14 :person/name "Danny Glover"] [56 :movie/cast 14]] [56 :movie/title "Predator 2"]] [56 :movie/cast 15]]
     [15 :person/name "Gary Busey"]]
    [[[14 :person/name "Danny Glover"] [64 :movie/cast 14]] [14 :person/name "Danny Glover"]]
    [[[14 :person/name "Danny Glover"] [58 :movie/cast 14]] [14 :person/name "Danny Glover"]]

    [[[[[14 :person/name "Danny Glover"] [57 :movie/cast 14]] [57 :movie/title "Lethal Weapon"]] [57 :movie/cast 14]]
     [14 :person/name "Danny Glover"]]

    [[[[[14 :person/name "Danny Glover"] [57 :movie/cast 14]] [57 :movie/title "Lethal Weapon"]] [57 :movie/cast 13]]
     [13 :person/name "Mel Gibson"]]

    [[[[[14 :person/name "Danny Glover"] [56 :movie/cast 14]] [56 :movie/title "Predator 2"]] [56 :movie/cast 36]]
     [36 :person/name "Ruben Blades"]]
    [[[14 :person/name "Danny Glover"] [56 :movie/cast 14]] [14 :person/name "Danny Glover"]]

    [[[[[14 :person/name "Danny Glover"] [57 :movie/cast 14]] [57 :movie/title "Lethal Weapon"]] [57 :movie/cast 15]]
     [15 :person/name "Gary Busey"]]

    [[[[[14 :person/name "Danny Glover"] [58 :movie/cast 14]] [58 :movie/title "Lethal Weapon 2"]] [58 :movie/cast 14]]
     [14 :person/name "Danny Glover"]]

    [[[[[14 :person/name "Danny Glover"] [64 :movie/cast 14]] [64 :movie/title "Lethal Weapon 3"]] [64 :movie/cast 37]]
     [37 :person/name "Joe Pesci"]]

    [[[[[14 :person/name "Danny Glover"] [56 :movie/cast 14]] [56 :movie/title "Predator 2"]] [56 :movie/cast 14]]
     [14 :person/name "Danny Glover"]]

    [[[[[14 :person/name "Danny Glover"] [58 :movie/cast 14]] [58 :movie/title "Lethal Weapon 2"]] [58 :movie/cast 13]]
     [13 :person/name "Mel Gibson"]]
    [[[14 :person/name "Danny Glover"] [57 :movie/cast 14]] [14 :person/name "Danny Glover"]]}

  ;new correct, 12 items, all uniform
  #{[[[[[14 :person/name "Danny Glover"] [58 :movie/cast 14]] [58 :movie/title "Lethal Weapon 2"]] [58 :movie/cast 37]]
     [37 :person/name "Joe Pesci"]]

    [[[[[14 :person/name "Danny Glover"] [64 :movie/cast 14]] [64 :movie/title "Lethal Weapon 3"]] [64 :movie/cast 14]]
     [14 :person/name "Danny Glover"]]

    [[[[[14 :person/name "Danny Glover"] [64 :movie/cast 14]] [64 :movie/title "Lethal Weapon 3"]] [64 :movie/cast 13]]
     [13 :person/name "Mel Gibson"]]

    [[[[[14 :person/name "Danny Glover"] [56 :movie/cast 14]] [56 :movie/title "Predator 2"]] [56 :movie/cast 15]]
     [15 :person/name "Gary Busey"]]

    [[[[[14 :person/name "Danny Glover"] [57 :movie/cast 14]] [57 :movie/title "Lethal Weapon"]] [57 :movie/cast 14]]
     [14 :person/name "Danny Glover"]]

    [[[[[14 :person/name "Danny Glover"] [57 :movie/cast 14]] [57 :movie/title "Lethal Weapon"]] [57 :movie/cast 13]]
     [13 :person/name "Mel Gibson"]]

    [[[[[14 :person/name "Danny Glover"] [56 :movie/cast 14]] [56 :movie/title "Predator 2"]] [56 :movie/cast 36]]
     [36 :person/name "Ruben Blades"]]

    [[[[[14 :person/name "Danny Glover"] [57 :movie/cast 14]] [57 :movie/title "Lethal Weapon"]] [57 :movie/cast 15]]
     [15 :person/name "Gary Busey"]]

    [[[[[14 :person/name "Danny Glover"] [58 :movie/cast 14]] [58 :movie/title "Lethal Weapon 2"]] [58 :movie/cast 14]]
     [14 :person/name "Danny Glover"]]

    [[[[[14 :person/name "Danny Glover"] [64 :movie/cast 14]] [64 :movie/title "Lethal Weapon 3"]] [64 :movie/cast 37]]
     [37 :person/name "Joe Pesci"]]

    [[[[[14 :person/name "Danny Glover"] [56 :movie/cast 14]] [56 :movie/title "Predator 2"]] [56 :movie/cast 14]]
     [14 :person/name "Danny Glover"]]

    [[[[[14 :person/name "Danny Glover"] [58 :movie/cast 14]] [58 :movie/title "Lethal Weapon 2"]] [58 :movie/cast 13]]
     [13 :person/name "Mel Gibson"]]}

  )
