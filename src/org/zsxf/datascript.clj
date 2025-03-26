(ns org.zsxf.datascript
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.zset :as zs]
            [org.zsxf.zset :as zset]
            [org.zsxf.xf :as xf]
            [datascript.core :as d]
            [org.zsxf.util :as util :refer [nth2]]
            [taoensso.timbre :as timbre]))

(defn tx-datoms->zset-of-maps
  "Transforms datoms into a zset of maps. Each map represents a datom with a weight."
  [datoms]
  (transduce
    (map (fn [[e a v t add-or-retract]]
           (let [weight (condp = add-or-retract true 1 false -1)]
             (zset/zset-item {:db/id e a v} weight))))
    conj
    #{}
    datoms))

(defn tx-datoms->zset
  "Transforms datoms into a zset of vectors. Each vector represents a datom with a weight."
  [datoms]
  (transduce
    (map (fn [[e a v _t add-or-retract]]
           (let [weight (condp = add-or-retract true 1 false -1)]
             (zset/zset-item [e a v] weight))))
    conj
    #{}
    datoms))

(defn datom->eid [datom]
  (if (vector? datom)
    (nth datom 0 nil)))

(defn datom->attr [datom]
  (if (vector? datom)
    (nth datom 1 nil)))

(defn datom-attr= [datom attr]
  (= (datom->attr datom) attr))

(defn datom->val [datom]
  (timbre/spy (meta datom))
  (if (vector? datom)
    (nth datom 2 nil)))

(defn datom->val-meta [datom]
  (with-meta
    (vector (datom->val datom))
    (meta datom)))

(defn datom-val= [datom value]
  (= (datom->val datom) value))

(defn datom-attr-val= [datom attr value]
  (and (datom-attr= datom attr) (datom-val= datom value)))

(defonce *conn (atom nil))


(comment
  (let [conn (d/create-conn {})]
    [
     (:tx-data
      (d/transact!
        conn
        [{:person/name "Alice"}]))
     ;=> [#datascript/Datom[1 :person/name "Alice" 536870913 true]]
     ; Add initial fact

     (:tx-data
      (d/transact! conn
        [[:db/add 1 :person/name "Alice"]]))
     ;=> []
     ; ... empty because the fact already exists

     (:tx-data
      (d/transact! conn
        [[:db/add 1 :person/born "USA"]]))
     ;=> [#datascript/Datom[1 :person/born "USA" 536870915 true]]
     ; ... :tx-data contains the new facts
     ])
  )

(comment
  ;movie example
  (let [schema {}
        conn   (d/create-conn schema)]

    [(d/transact! conn
       [{:person/name "Alice"}])

     (d/transact! conn
       [{:movie/director 1
         :movie/title    "RoboCop 1"}])

     (d/transact! conn
       [{:movie/director 1
         :movie/title    "RoboCop 2"}])

     (d/transact! conn
       [{:person/name "Bob"}])

     (d/transact! conn
       [{:movie/director 4
         :movie/title    "LOTR 1"}])

     (d/transact! conn
       [{:movie/director 4
         :movie/title    "LOTR 2"}])

     (d/transact! conn
       [{:movie/director 4
         :movie/title    "LOTR 3"}])

     ]

    ;query
    (into {}
      (d/q
        '[:find ?name (count ?m)
          :where
          [?p :person/name ?name]
          [?m :movie/title]
          [?m :movie/director ?p]]
        @conn))
    )
  )

(comment
  (let [schema {:team/name   {:db/cardinality :db.cardinality/one
                              :db/unique      :db.unique/identity}
                :player/name {:db/cardinality :db.cardinality/one
                              :db/unique      :db.unique/identity}
                :player/team {:db/cardinality :db.cardinality/one
                              :db/valueType   :db.type/ref}}
        ;        schema {}
        conn   (d/create-conn schema)]

    (d/transact! conn
      [{:team/name "T"}])
    (d/transact! conn
      [{:team/name "T2"}])

    (d/transact! conn
      [{:player/name "P"}])
    (d/transact! conn
      [{:player/name "P2"}])

    (d/transact! conn
      [{:player/name "P"
        :player/team [:team/name "T"]}])

    (d/q
      '[:find ?t
        ;:in $ ?team-name
        :where
        [?p :player/team ?t]
        [?t :team/name "T"]
        [?p :player/name "P"]]

      (comment
        (and
          [?p :player/team ?t])

        [?p :player/team [?t :team/name "T"] 0])

      @conn
      ;?team-name
      )
    ))

(comment
  ;case 1
  (let [schema {:person/friend {:db/cardinality :db.cardinality/many
                                :db/valueType   :db.type/ref}}
        conn   (d/create-conn schema)]

    [(d/transact! conn
       [{:person/name "Alice"}])
     (d/transact! conn
       [{:person/name "Bob"}])
     (d/transact! conn
       [{:db/id 1 :person/friend 2}])
     (d/transact! conn
       [{:db/id 1 :person/friend 1}])

     ;query
     (d/q
       '[:find ?v2
         :where
         [?p :person/name ?v]
         [?p :person/friend ?p2]
         [?p2 :person/name ?v2]]
       @conn)]
    ;query returns:
    ;=> #{["Alice"] ["Bob"]}
    )

  ;case 2 (notice the change in query)
  (let [schema {:person/friend {:db/cardinality :db.cardinality/many
                                :db/valueType   :db.type/ref}}
        conn   (d/create-conn schema)]

    [(d/transact! conn
       [{:person/name "Alice"}])
     (d/transact! conn
       [{:person/name "Bob"}])
     (d/transact! conn
       [{:db/id 1 :person/friend 2}])
     (d/transact! conn
       [{:db/id 1 :person/friend 1}])

     ;query
     (d/q
       '[:find ?v
         :where
         [?p :person/name ?v]
         [?p :person/friend ?p2]
         [?p2 :person/name ?v]]
       @conn)]
    ;query returns:
    ;=> #{["Alice"]}
    ))

(comment
  (let [schema {}
        conn   (d/create-conn schema)]

    [(d/transact! conn
       [{:person/name "Alice"}])
     (d/transact! conn
       [{:person/name "Bob"}])
     (d/transact! conn
       [{:person/name "Clark"}])
     (d/transact! conn
       [{:person/name "Alice"}])]

    ;query
    (d/q
      '[:find ?p1 ?name
        :where
        [?p1 :person/name ?name]
        [?p2 :person/name ?name]
        [(not= ?p1 ?p2)]
        ]
      @conn)
    )
  )

(comment

  (d/q
    '[:find ?e ?t
      ;:in $ ?team-name
      :where
      [?e :player/name]
      [?t :team/name]]
    @conn
    ;?team-name
    )

  (d/q
    '[:find ?e
      ;:in $ ?team-name
      :where
      [?e :player/team ?t]
      [?e :player/city "NY"]
      [?t :team/name "A"]
      [?t :team/color "red"]
      ]
    @conn
    ;?team-name
    )

  ;TODO how does DBSP compute a query like this one:
  (d/q
    '[:find ?e
      ;:in $ ?team-name
      :where
      [?e :player/team ?t]
      [?e :player/city "NY"]]
    @conn
    ;?team-name
    ))

(defonce index-state-all (atom {}))
(defonce result-deltas (atom []))
(defonce result-set (atom #{}))
(defonce result-state (atom nil))

(comment
  ;transform result set to datascript form
  (into #{}
    (map (fn [result-set-item] [(:db/id (first result-set-item))]))
    @result-set))

(defonce input (atom (a/chan)))

(defn print-index-state []
  (timbre/spy @index-state-all)
  nil)


(defn reset-state! []
  (do
    (reset! index-state-all {})
    (reset! result-deltas [])
    (reset! result-set #{})
    (reset! result-state nil)
    (reset! input (a/chan)))
  )

(comment
  (print-index-state)
  (do
    (comment
      (let [xf        (comp
                        (xf/mapcat-zset-transaction-xf)
                        (let [pred-1 #(datom-attr= % :person/name)
                              pred-2 #(datom-attr= % :person/friend)
                              pred-3 #(datom-attr= (second %) :person/friend)
                              pred-4 #(datom-attr= % :person/name)]
                          (comp
                            ;ignore datoms irrelevant to the query
                            (map (fn [zset]
                                   (xf/disj-irrelevant-items
                                     zset pred-1 pred-2 pred-3 pred-4)))
                            (map (fn [tx-current-item] (timbre/spy tx-current-item)))
                            (xf/join-xf
                              pred-1 datom->eid
                              pred-2 datom->eid
                              index-state-all)
                            (map (fn [zset-in-between] (timbre/spy zset-in-between)))
                            (xf/join-xf
                              pred-3 #(-> % second datom->val)
                              pred-4 datom->eid
                              index-state-all
                              :last? true
                              ;TODO make this simpler _and_ easier?
                              :return-zset-item-xf (filter #(= (timbre/spy (-> % first first datom->val))
                                                              (timbre/spy (-> % second datom->val)))))
                            (map (fn [zset-in-between-last] (timbre/spy zset-in-between-last)))
                            (xforms/reduce zs/zset+))))
            output-ch (a/chan (a/sliding-buffer 1)
                        (xf/query-result-set-xf result-set))
            to        (a/pipeline 1 output-ch xf @input)]
        @input))


    (a/>!!
      @input
      [(tx-datoms->zset
         [[1 :person/name "Alice" :t true]])])

    (a/>!!
      @input
      [(tx-datoms->zset
         [[2 :person/name "Bob" :t true]])])

    (a/>!!
      @input
      [(tx-datoms->zset
         [[1 :person/friend 2 :t true]])])

    (a/>!!
      @input
      [(tx-datoms->zset
         [[1 :person/friend 1 :t true]])])

    #{
      [
       ;[?p :person/name ?name] -> [?m :movie/director ?p], joining them returns a joined pair like:
       [[1 :person/name "Alice"]
        [2 :movie/director 1]]

       ;[?m :movie/director ?p] -> ^^^^^^^^^^^^^^^^^^^^^^^ pred-3 looking for the second item from the joined pair)
       [2 :movie/title "RoboCop"]
       ]}


    (comment
      '(d/q
         '[:find ?name (count ?m)
           :where
           [?p :person/name ?name]
           [?m :movie/title]
           [?m :movie/director ?p]
           [?p :person/born "USA"]]
         @conn)
      (let [xf        (comp
                        (xf/mapcat-zset-transaction-xf)
                        (let [pred-1 #(datom-attr= % :person/name)
                              pred-2 #(datom-attr= % :movie/director)
                              pred-3 #(datom-attr= % :movie/director)
                              pred-4 #(datom-attr= % :movie/title)
                              pred-5 #(datom-attr= (-> % (nth2 0) (nth2 0)) :person/name)
                              pred-6 #(datom-attr-val= % :person/born "USA")]
                          (comp
                            ;ignore datoms irrelevant to the query
                            (map (fn [zset]
                                   (xf/disj-irrelevant-items
                                     zset pred-1 pred-2 pred-3 pred-4 pred-5 pred-6)))
                            (map (fn [tx-current-item] (timbre/spy tx-current-item)))
                            (xf/join-xf
                              pred-1 datom->eid
                              pred-2 datom->val
                              index-state-all)
                            (map (fn [zset-in-between] (timbre/spy zset-in-between)))
                            (xf/join-right-pred-1-xf
                              pred-3 datom->eid
                              pred-4 datom->eid
                              index-state-all)
                            (xf/join-xf
                              pred-5 #(-> % (nth2 0) (nth2 0) datom->eid)
                              pred-6 datom->eid
                              index-state-all
                              :last? true)
                            (map (fn [zset-in-between-last] (timbre/spy zset-in-between-last)))
                            ;find
                            (xforms/reduce zs/zset+)
                            (map (fn [post-reduce] (timbre/spy post-reduce)))
                            #_(xf/group-by-count-xf
                              (fn [zset-item]
                                (-> zset-item (nth2 0) (nth2 0) (nth2 0) datom->val))))))
            output-ch (a/chan (a/sliding-buffer 1)
                        (comp
                          (map (fn [delta]
                                 (swap! result-deltas conj delta)
                                 delta))                    ;debug
                          (xf/query-result-state-xf result-state)
                          ))
            to        (a/pipeline 1 output-ch xf @input)]
        @input)

      (a/>!!
        @input
        [(tx-datoms->zset
           [[1 :person/name "Alice" :t true]
            [2 :movie/director 1 :t true]
            [2 :movie/title "RoboCop" :t true]
            [1 :person/born "USA" :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[3 :person/name "Bob" :t true]
            [4 :movie/director 3 :t true]
            [4 :movie/title "RoboCop" :t true]
            [3 :person/born "USA" :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[4 :movie/director 3 :t false]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[3 :person/name "Bob" :t false]
            [4 :movie/title "RoboCop" :t false]])])

      (comment
        (/ 8 2)
        (/ 27 3)
        (/ 64 4)
        (/ 125 5)
        )

      (a/>!!
        @input
        [(tx-datoms->zset
           [[4 :movie/title "RoboCop" :t true]])])


      (a/>!!
        @input
        [(tx-datoms->zset
           [[3 :person/name "Bob" :t false]
            [4 :movie/director 3 :t false]
            [4 :movie/title "RoboCop" :t false]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[4 :movie/title "RoboCop" :t true]])])


      (a/>!!
        @input
        [(tx-datoms->zset
           [[2 :movie/director 1 :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[2 :movie/director 1 :t false]])
         (tx-datoms->zset
           [[2 :movie/director 1 :t false]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[1 :person/name "Alice" :t true]
            [2 :movie/director 1 :t true]
            [2 :movie/title "RoboCop" :t true]])
         (tx-datoms->zset
           [[3 :movie/title "Matrix" :t true]
            [3 :movie/director 1 :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[1 :person/name "Alice" :t false]
            [2 :movie/director 1 :t false]
            [2 :movie/title "RoboCop" :t false]])
         (tx-datoms->zset
           [[1 :person/name "Alice" :t false]
            [2 :movie/director 1 :t false]
            [2 :movie/title "RoboCop" :t false]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[3 :movie/title "Matrix" :t true]
            [3 :movie/director 1 :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[3 :movie/title "Matrix" :t false]
            [3 :movie/director 1 :t false]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[1 :person/name "Alice" :t false]
            [2 :movie/director 1 :t false]
            [2 :movie/title "RoboCop" :t false]])
         (tx-datoms->zset
           [[3 :person/name "Bob" :t false]
            [2 :movie/director 3 :t false]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[1 :person/name "Alice" :t false]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[2 :movie/director 1 :t false]
            [2 :movie/title "RoboCop" :t false]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[3 :person/name "Bob" :t true]
            [2 :movie/director 3 :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[3 :person/name "Bob" :t false]
            [2 :movie/director 3 :t false]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[5 :movie/director 3 :t true]
            [5 :movie/title "RoboCop" :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[3 :person/name "Bob" :t false]
            [4 :movie/director 3 :t false]
            [4 :movie/title "RoboCop" :t false]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[4 :movie/title "RoboCop" :t false]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[3 :person/name "Bob" :t false]])])
      (a/>!!
        @input
        [(tx-datoms->zset
           [[4 :movie/director 3 :t true]])])


      (a/>!!
        @input
        [(tx-datoms->zset
           [[3 :person/name "Bob" :t false]
            [4 :movie/director 3 :t false]
            [4 :movie/title "RoboCop" :t false]
            ])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[1 :person/name "Alice" :t false]
            [2 :movie/director 1 :t false]
            [2 :movie/title "RoboCop" :t false]])])

      )


    (comment
      (d/q
        '[:find ?name
          :with ?p1
          :where
          [?p1 :person/name ?name]
          [?p2 :person/name ?name]
          [(not= ?p1 ?p2)]
          ]
        @conn)

      (let [xf        (comp
                        (xf/mapcat-zset-transaction-xf)
                        (let [pred-1  #(datom-attr= % :person/name)
                              find-xf (map (xf/with-meta-f
                                             (fn [rel1+rel2]
                                               (timbre/spy rel1+rel2)
                                               (timbre/spy
                                                 [(datom->val (first rel1+rel2))]))))]
                          (comp
                            ;ignore datoms irrelevant to the query
                            (map (fn [zset]
                                   (xf/disj-irrelevant-items
                                     zset pred-1)))
                            (map (fn [tx-current-item] (timbre/spy tx-current-item)))
                            (xf/join-xf
                              pred-1 datom->val
                              pred-1 datom->val
                              index-state-all
                              :last? true
                              :return-zset-item-xf
                              (comp
                                (filter
                                  (fn [rel1+rel2]
                                    (not=
                                      (timbre/spy (datom->eid (first rel1+rel2)))
                                      (timbre/spy (datom->eid (second rel1+rel2))))))))
                            (map (fn [zset-in-between-last] (timbre/spy zset-in-between-last)))
                            (xforms/reduce (zs/zset-xf+ find-xf)))))
            output-ch (a/chan (a/sliding-buffer 1)
                        (xf/query-result-set-xf result-set))
            to        (a/pipeline 1 output-ch xf @input)]
        @input)

      (a/>!!
        @input
        [(tx-datoms->zset
           [[1 :person/name "Alice" :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[2 :person/name "Bob" :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[3 :person/name "Alice" :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset
           [[1 :person/name "Alice" :t false]])])

      )

    ))
