(ns org.zsxf.query-test
  (:require
   #?(:clj  [clojure.test :refer [deftest is]]
      :cljs [cljs.test :refer-macros [deftest is]])
   #?(:clj [clj-memory-meter.core :as mm])
   [datascript.core :as d]
   [datascript.db :as ddb]
   [medley.core :as medley]
   [net.cgrand.xforms :as xforms]
   [org.zsxf.datascript :as ds]
   [clojure.set :as set]
   [org.zsxf.datom2 :as d2]
   [org.zsxf.lib.result-set-xf.core :as rsxf]
   [org.zsxf.query :as q]
   [org.zsxf.datalog.compiler]
   [org.zsxf.relation :as rel]
   [org.zsxf.util :as util :refer [nth2 path-f]]
   [org.zsxf.xf :as xf]
   [org.zsxf.zset :as zs]
   #?(:clj [taoensso.nippy :as nippy])
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
      {:clause    '[?e :team/name "A"]
       :pred      #(d2/datom-attr-val= % :team/name "A")
       :index-kfn d2/datom->eid}
      {:clause    '[?e :event/country ?country]
       :pred      #(d2/datom-attr= % :event/country)
       :index-kfn d2/datom->eid}
      query-state)
    (xf/join-xf
      {:clause    '[?e :event/country ?country]
       :path      (util/path-f [1])
       :pred      #(d2/datom-attr= % :event/country)
       :index-kfn d2/datom->eid}
      {:clause    '[?e :team/points-scored ?pts]
       :pred      #(d2/datom-attr= % :team/points-scored)
       :index-kfn d2/datom->eid}
      query-state
      :last? true)
    (xforms/reduce zs/zset+)
    ;group by aggregates
    (xf/group-by-xf
      #(-> % (util/nth2 0) (util/nth2 1) d2/datom->val)
      (comp
        (xforms/transjuxt {:sum (xforms/reduce
                                  (zs/zset-sum+
                                    #(-> % (util/nth2 1) d2/datom->val)))
                           :cnt (xforms/reduce zs/zset-count+)})
        (mapcat (fn [{:keys [sum cnt]}]
                  [(zs/zset-sum-item sum)
                   (zs/zset-count-item cnt)]))))
    (map (fn [final-xf-delta] (timbre/spy final-xf-delta)))))

(deftest simple-aggregate-1
  (let [query-1  (q/create-query aggregate-example-xf)
        _        (q/input query-1
                   [(d2/tx-datoms->datoms2->zset
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
        result-1 (q/get-aggregate-result query-1)
        _        (q/input query-1
                   [(d2/tx-datoms->datoms2->zset
                      [(ddb/datom 1 :team/name "A" 536870913 false)
                       (ddb/datom 2 :team/name "A" 536870913 false)
                       (ddb/datom 3 :team/name "A" 536870913 false)])])
        result-2 (q/get-aggregate-result query-1)]
    (is
      (= result-1
        {"Japan" #{[:sum 43] [:count 2]}, "Australia" #{[:sum 29] [:count 2]}}))
    (is
      (= result-2
        {"Japan" #{}, "Australia" #{[:sum 4] [:count 1]}}))))

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
       :pred      #(d2/datom-attr-val= % :person/name "Alice")
       :index-kfn d2/datom->eid}
      {:clause    '[?p :person/country ?c]
       :pred      #(d2/datom-attr= % :person/country)
       :index-kfn d2/datom->eid}
      query-state)
    (xf/join-xf
      {:clause    '[?p :person/country ?c]
       :path      (util/path-f [1])
       :pred      #(d2/datom-attr= % :person/country)
       :index-kfn d2/datom->val}
      {:clause    '[?c :country/continent "Europe"]
       :pred      #(d2/datom-attr-val= % :country/continent "Europe")
       :index-kfn d2/datom->eid}
      query-state
      :last? true)
    ;(xf/join-xf-3
    ;  '[?p :person/name "Alice"]
    ;  [(path-f [0 0])
    ;   #(d2/datom-attr= % :person/name)] ds/datom->eid
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
        datoms (d2/tx-datoms->datoms2->zsets
                 [(ddb/datom 2 :person/country 1 536870913 true)
                  (ddb/datom 2 :person/name "Alice" 536870913 true)
                  (ddb/datom 1 :country/continent "Europe" 536870913 true)])]
    (q/input query datoms)
    (q/get-result query)))

(comment
  ;example usage
  (def query-1 (q/create-query person-city-country-example-xf-join-3))


  (q/input query-1
    [(d2/tx-datoms->datoms2->zset
       [(ddb/datom 1 :country/continent "Europe" 536870913 true)
        (ddb/datom 2 :person/name "Alice" 536870913 true)
        (ddb/datom 2 :likes "pizza" 536870913 true)
        (ddb/datom 2 :person/country 1 536870913 true)])])

  (q/get-result query-1)

  (rel/find-clause
    (first (q/get-result query-1))
    '[?p :person/name "Alice"])

  (q/input query-1
    [(d2/tx-datoms->datoms2->zset
       [(ddb/datom 1 :team/name "A" 536870913 false)
        (ddb/datom 2 :team/name "A" 536870913 false)
        (ddb/datom 3 :team/name "A" 536870913 false)])])

  (q/get-result query-1)

  (q/get-state query-1)

  )

(defn new-join-xf-3
  [query-state]
  (comp
    (xf/mapcat-zset-transaction-xf)
    ;danny
    (xf/join-xf
      {:clause    '[?danny :person/name "Danny Glover"]
       :path      (path-f [])
       :pred      #(d2/datom-attr-val= % :person/name "Danny Glover")
       :index-kfn d2/datom->eid}
      {:clause    '[?m :movie/cast ?danny]
       :path      (path-f [])
       :pred      #(d2/datom-attr= % :movie/cast)
       :index-kfn d2/datom->val}
      query-state)
    ;movie cast
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    (xf/join-xf
      {:clause    '[?m :movie/cast ?danny]
       :path      (path-f [1])
       :pred      #(d2/datom-attr= % :movie/cast)
       :index-kfn d2/datom->eid}
      {:clause    '[?m :movie/title ?title]
       :pred      #(d2/datom-attr= % :movie/title)
       :index-kfn d2/datom->eid}
      query-state)
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    ;actors, movie cast
    (xf/join-xf
      {:clause    '[?m :movie/title ?title]
       :path      (path-f [1])
       :pred      #(d2/datom-attr= % :movie/title)
       :index-kfn d2/datom->eid}
      {:clause    '[?m :movie/cast ?a]
       :pred      #(d2/datom-attr= % :movie/cast)
       :index-kfn d2/datom->eid}
      query-state)
    (map (fn [zset-in-between] (timbre/spy zset-in-between)))
    (xf/join-xf
      {:clause    '[?m :movie/cast ?a]
       :path      (path-f [1])
       :pred      #(d2/datom-attr= % :movie/cast)
       :index-kfn d2/datom->val}
      {:clause    '[?a :person/name ?actor]
       :pred      #(d2/datom-attr= % :person/name)
       :index-kfn d2/datom->eid}
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
        [conn _schema] (util/load-learn-db)
        query-1 (q/create-query new-join-xf-3)
        _       (ds/init-query-with-conn query-1 conn)
        result  (q/get-result query-1)]
    (is (=
          (count (d/q datalog-query-1 @conn))
          (count result)))))

(defn count-artists-by-countries-all
  "Query for artist count by all countries."
  [query-state]
  (let [pred-1 #(d2/datom-attr= % :country/name-alpha-2)
        pred-2 #(d2/datom-attr= % :artist/country)]
    (comp
      (xf/mapcat-zset-transaction-xf)
      (map (fn [zset] (xf/disj-irrelevant-items zset pred-1 pred-2)))
      (xf/join-xf
        {:clause    [:c1]
         :pred      pred-1
         :index-kfn d2/datom->eid}
        {:clause    [:c2]
         :pred      pred-2
         :index-kfn d2/datom->val}
        query-state
        :last? true)
      (xforms/reduce zs/zset+)
      ;group by aggregates
      (xf/group-by-xf
        #(-> % (nth2 0) d2/datom->val)
        xf/group-by-count-xform))))

#?(:clj
   (defn thaw-artist-datoms!
     "Returns a vector of thawed datoms from the nippy file."
     []
     (try
       (nippy/thaw-from-file
         "resources/mbrainz/artists_datoms.nippy"
         {:thaw-xform
          (comp
            (map (fn [thawing]
                   (if (vector? thawing)
                     (let [[e a v tx b] thawing]
                       (d/datom e a v tx b))
                     thawing))))})
       (catch Throwable _e (timbre/info "Cannot load artist datoms. Missing a nippy data file?") nil))))

#?(:clj
   (defn init-artist-db->conn [artists-datoms]
     (d/conn-from-db
       (d/init-db artists-datoms
         {:artist/name          {:db/cardinality :db.cardinality/one}
          :artist/id            {:db/cardinality :db.cardinality/one
                                 :db/unique      :db.unique/identity}
          :artist/genres        {:db/cardinality :db.cardinality/many
                                 :db/valueType   :db.type/ref}
          :genre/name           {:db/cardinality :db.cardinality/one
                                 :db/unique      :db.unique/identity}
          :artist/type          {:db/cardinality :db.cardinality/one}
          :artist/country       {:db/cardinality :db.cardinality/one
                                 :db/valueType   :db.type/ref}
          :country/name-alpha-2 {:db/cardinality :db.cardinality/one
                                 :db/unique      :db.unique/identity}}))))

#?(:clj
   (deftest mbrainz-aggregates-test
     (if-let [artist-datoms (thaw-artist-datoms!)]
       (let [conn                      (init-artist-db->conn artist-datoms)
             query                     (q/create-query count-artists-by-countries-all)
             _                         (time (ds/init-query-with-conn query conn :listen? false))
             result                    (q/get-result query)
             result-edn-path           "resources/mbrainz/expected_result/count-artists-by-countries-all.edn"
             result-from-file          (util/read-edn-file result-edn-path)
             query-size                (mm/measure query :bytes true)
             query-size-in-mb          (util/megabytes query-size)
             expected-query-size-in-mb 116]
         (when (<= 24 (util/jvm-version))
           ;check that impl changes haven't increased the query size
           (is (<= query-size-in-mb expected-query-size-in-mb)))
         ;check query result
         (is (= result result-from-file)))
       (do
         (timbre/info "Test will skip, no artist datoms found.")
         true))))

(defn cartesian-product-movie-person-zsxf [query-state]
  (comp
    (xf/mapcat-zset-transaction-xf)
    (xf/cartesian-xf
      {:clause '[?m :movie/title _]
       :pred   #(d2/datom-attr= % :movie/title)}
      {:clause '[?m :person/name _]
       :pred   #(d2/datom-attr= % :person/name)}
      query-state
      :last? true)
    (xforms/reduce
      (zs/zset-xf+
        (map (xf/same-meta-f
               (fn [zset-item]
                 (let [juxt-find (juxt
                                   (comp d2/datom->eid (util/path-f [0]))
                                   (comp d2/datom->eid (util/path-f [1])))]
                   (juxt-find zset-item)))))))))

(defn cartesian-product-movie-movie-zsxf [query-state]
  (comp
    (xf/mapcat-zset-transaction-xf)
    (xf/cartesian-xf
      {:clause '[?m :movie/title _]
       :pred   #(d2/datom-attr= % :movie/title)}
      {:clause '[?m :movie/title _x]
       :pred   #(d2/datom-attr= % :movie/title)}
      query-state
      :last? true)
    (xforms/reduce
      (zs/zset-xf+
        (map (xf/same-meta-f
               (fn [zset-item]
                 (let [juxt-find (juxt
                                   (comp d2/datom->val (util/path-f [0]))
                                   (comp d2/datom->val (util/path-f [1])))]
                   (juxt-find zset-item)))))))))

(def cartesian-product-movie-person-ds
  '[:find ?m ?p
    :where
    [?m :movie/title _]
    [?p :person/name _]])

(def cartesian-product-movie-movie-ds
  '[:find ?t1 ?t2
    :where
    [?m :movie/title ?t1]
    [?m2 :movie/title ?t2]])

(deftest cartesian-product-movie-movie
  (let [[conn _] (util/load-learn-db)
        query       (q/create-query cartesian-product-movie-movie-zsxf)
        _           (ds/init-query-with-conn query conn)
        result-zsxf (q/get-result query)
        result-ds   (d/q cartesian-product-movie-movie-ds @conn)]
    (is (= result-ds result-zsxf))))

(deftest cartesian-product-movie-movie-with-retract
  (let [[conn _] (util/load-learn-db)
        query       (q/create-query cartesian-product-movie-movie-zsxf)
        _           (ds/init-query-with-conn query conn)
        _           (d/transact! conn [[:db/retract 52 :movie/title]])
        result-zsxf (q/get-result query)
        result-ds   (d/q cartesian-product-movie-movie-ds @conn)]
    (is (= result-ds result-zsxf))))

;equivalent?
(comment
  ;OR is set/union TODO
  '(or
     ;all movies with sequels (10), inner-join, join-xf
     (and
       [?m :movie/title ?t]
       [?m :movie/sequel])
     ;movies without sequels (2), set/diff TODO
     (and
       [?m :movie/title ?t]
       (not [?m :movie/sequel])))
  )


(deftest cartesian-product-movie-person
  (let [[conn _] (util/load-learn-db)
        query       (q/create-query cartesian-product-movie-person-zsxf)
        _           (ds/init-query-with-conn query conn)
        result-zsxf (q/get-result query)
        result-ds   (d/q cartesian-product-movie-person-ds @conn)]
    (is (= result-ds result-zsxf))))

(deftest cartesian-product-movie-person-retraction
  (let [[conn _schema] (util/load-learn-db)
        query         (q/create-query cartesian-product-movie-person-zsxf)
        _             (ds/init-query-with-conn query conn)
        result-ds-1   (d/q cartesian-product-movie-person-ds @conn)
        result-zsxf-1 (q/get-result query)
        eid-retract   (first (d/q '[:find [?p] :where [?p :person/name "Danny Glover"]] @conn))
        _             (d/transact! conn [[:db/retract eid-retract :person/name "Danny Glover"]])
        result-ds-2   (d/q cartesian-product-movie-person-ds @conn)
        result-zsxf-2 (q/get-result query)]
    (is (= result-ds-1 result-zsxf-1))
    (is (= result-ds-2 result-zsxf-2))))


(def cartesian-product-danny-xf
  (fn [state]
    (comp
      (xf/mapcat-zset-transaction-xf)
      ;rel1
      (xf/join-xf
        {:clause    '[?danny :person/name "Danny Glover"],
         :path      identity,
         :pred      #(d2/datom-attr-val= % :person/name "Danny Glover")
         :index-kfn d2/datom->eid,}
        {:clause    '[?danny :person/born ?danny-born],
         :path      identity,
         :pred      #(d2/datom-attr= % :person/born),
         :index-kfn org.zsxf.datom2/datom->eid}
        state)

      ;rel2
      (xf/join-xf
        {:clause    '[?a :person/name ?actor]
         :path      identity
         :pred      #(d2/datom-attr= % :person/name)
         :index-kfn d2/datom->eid}
        {:clause    '[?a :person/born ?actor-born]
         :path      identity,
         :pred      #(d2/datom-attr= % :person/born)
         :index-kfn d2/datom->eid}
        state)
      (xf/join-xf
        {:clause    '[?a :person/name ?actor]
         :path      (util/path-f [0])
         :pred      #(org.zsxf.datom2/datom-attr= % :person/name)
         :index-kfn d2/datom->eid}
        {:clause    '[_ :movie/cast ?a]
         :path      identity
         :pred      #(org.zsxf.datom2/datom-attr= % :movie/cast)
         :index-kfn d2/datom->val}
        state)
      ;debug
      (map (fn [pre-cartesian] pre-cartesian))

      ;cartesian product rel1 and rel2
      (xf/cartesian-xf
        {:clause '[?danny :person/born ?danny-born]
         :path   (util/path-f [1])
         :pred   #(org.zsxf.datom2/datom-attr= % :person/born)}
        {:clause '[?a :person/born ?actor-born]
         :path   (util/path-f [0 1])
         :pred   #(org.zsxf.datom2/datom-attr= % :person/born)}
        state
        :last? true)
      ;debug
      (map (fn [post-cartesian] post-cartesian))
      (xforms/reduce
        (zs/zset-xf+
          (map (xf/same-meta-f
                 (fn [zset-item]
                   (let [find-rel-result
                         ;new, find via index
                                          (vector
                                            (-> zset-item
                                              (rel/find-clause '[?danny :person/born ?danny-born])
                                              d2/datom->val)
                                            (-> zset-item
                                              (rel/find-clause '[?a :person/born ?actor-born])
                                              d2/datom->val))
                         ;old path approach
                         juxt-path-result ((juxt
                                             (comp d2/datom->val (util/path-f [0 1]))
                                             (comp d2/datom->val (util/path-f [1 0 1])))
                                           zset-item)]
                     ;(timbre/spy (= find-rel-result juxt-path-result))
                     find-rel-result)))))))))

(def cartesian-product-danny-ds
  '[:find ?danny-born ?actor-born
    :where
    [?danny :person/name "Danny Glover"]
    [?danny :person/born ?danny-born]

    [?a :person/name ?actor]
    [?a :person/born ?actor-born]
    [_ :movie/cast ?a]
    ])

(deftest cartesian-danny
  (let [[conn _schema] (util/load-learn-db)
        query         (q/create-query cartesian-product-danny-xf)
        _             (ds/init-query-with-conn query conn)
        result-zsxf-1 (q/get-result query)
        result-ds-1   (d/q cartesian-product-danny-ds @conn)
        ]
    (is (= result-ds-1 result-zsxf-1))))

(def tiny-data
  [{:db/id        -200
    :movie/title  "The Terminator"
    :movie/year   1984
    :movie/sequel -207}
   ;{:db/id          -201
   ; :movie/title    "First Blood"
   ; :movie/year     1982}
   {:db/id       -207
    :movie/title "Terminator 2: Judgment Day"
    :movie/year  1991}])

(def movies-maybe-sequels-ds
  '[:find ?title ?maybe-sequel
    :in $
    :where
    [?m :movie/title ?title]
    [(get-else $ ?m :movie/sequel :no-sequel) ?maybe-sequel]
    [(get-else $ ?maybe-sequel :movie/title :no-sequel-no-title) ?maybe-sequel-title]])

(defn movies-without-sequels-zsxf [query-state]
  (comp
    (xf/mapcat-zset-transaction-xf)
    (xf/join-xf
      {:clause    '[?m :movie/title ?title]
       :path      identity
       :pred      #(d2/datom-attr= % :movie/title)
       :index-kfn d2/datom->eid}
      {:clause    '[?m :movie/sequel]
       :path      identity
       :pred      #(d2/datom-attr= % :movie/sequel)
       :index-kfn d2/datom->eid}
      query-state)
    (xf/difference-xf
      {:clause      '[?m :movie/title ?title]
       :path        identity
       :pred        #(d2/datom-attr= % :movie/title)
       :zset-item-f identity}
      {:clause      '[?m :movie/sequel]
       :path        (util/path-f [1])
       :pred        #(d2/datom-attr= % :movie/sequel)
       :zset-item-f first}
      :last? true)
    (xforms/reduce
      (zs/zset-xf+
        (map (xf/same-meta-f
               (fn [zset-item]
                 zset-item
                 [(d2/datom->val zset-item)])))))))

(def movies-without-sequels-ds
  '[:find ?title
    :where
    [?m :movie/title ?title]
    (not [?m :movie/sequel ?sequel])])

(deftest movies-without-sequels
  (let [_ (timbre/set-min-level! :trace)
        [conn _] (util/load-learn-db-empty)
        [conn _] (util/load-learn-db)
        query       (q/create-query movies-without-sequels-zsxf)
        _           (ds/init-query-with-conn query conn)
        result-zsxf (q/get-result query)
        result-ds   (d/q movies-without-sequels-ds @conn)]
    ;(def conn conn)
    ;(def query query)
    (is (= result-ds result-zsxf)))

  (comment

    (d/transact! conn
      [{:movie/title "Terminator"}])

    (d/q movies-without-sequels-ds @conn)
    (q/get-result query)

    (d/transact! conn
      [{:movie/title "Terminator 2"}])

    (d/q movies-without-sequels-ds @conn)
    (q/get-result query)

    (d/transact! conn
      [{:db/id 1 :movie/sequel 2}])

    (d/q movies-without-sequels-ds @conn)
    (q/get-result query)

    (d/transact! conn
      [{:movie/title "Terminator 3"}])

    (d/q movies-without-sequels-ds @conn)

    (d/transact! conn
      [{:db/id 2 :movie/sequel 3}])

    (d/q movies-without-sequels-ds @conn)

    ))

(def all-movies-optionally-find-sequels-ds
  '[:find ?title ?m2 #_?title2
    :in $
    :where
    [?m :movie/title ?title]
    [(get-else $ ?m :movie/sequel [:not-found]) ?m2]
    ;[(get-else $ ?m2 :movie/title :no-seq-no-title) ?title2]
    ])

(def all-movies-optionally-find-sequel-title-ds
  '[:find (pull ?m [:db/id :movie/title {:movie/sequel [:movie/title]}])
    :where
    [?m :movie/title ?title]
    ])

;TODO how do we handle this?
;reverse pull example
(def all-movies-optionally-find-sequels-and-prequels-ds
  '[:find (pull ?m [:db/id :movie/title {:movie/sequel [:movie/title]} {:movie/_sequel [:movie/title]}])
    :where
    [?m :movie/title ?title]
    ])
(comment
  (let [[conn _schema] (util/load-learn-db)
        result-ds-1 (d/q all-movies-optionally-find-sequels-and-prequels-ds @conn)]
    result-ds-1)
  )

(def union-movies-people-ds
  '[:find ?x
    :where
    (or
      [_ :movie/title ?x]
      [_ :person/name ?x])
    ])

(defn union-explore []
  (let [[conn _schema] (util/load-learn-db)
        result-ds (d/q union-movies-people-ds @conn)]
    result-ds)
  )
