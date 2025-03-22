(ns org.zsxf.datascript
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.zset :as zs]
            [org.zsxf.zset :as zset]
            [org.zsxf.xf :as xf]
            [datascript.core :as d]
            [taoensso.timbre :as timbre]))


(defn tx-datoms->zset
  "Transforms datoms into a zset of maps. Each map represents a datom with a weight."
  [datoms]
  (transduce
    (map (fn [[e a v t add-or-retract]]
           (let [weight (condp = add-or-retract true 1 false -1)]
             (zset/zset-item {:db/id e a v} weight))))
    conj
    #{}
    datoms))

(defn tx-datoms->zset-2
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
  (if (vector? datom)
    (nth datom 2 nil)))

(defn datom-val= [datom value]
  (= (datom->val datom) value))

(defn datom-attr-val= [datom attr value]
  (and (datom-attr= datom attr) (datom-val= datom value)))

(defonce *conn (atom nil))

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
(defonce result-set (atom #{}))

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
    (reset! result-set #{})
    (reset! input (a/chan)))
  )

(comment

  (print-index-state)

  (do

    ;with map-datoms
    ;equivalent query
    '[:find ?p                                              ;find is wip
      :where
      [?p :player/team ?t]
      [?t :team/name "A"]
      [?t :team/color "red"]
      [?p :player/city ?c]
      [?c :city/name "NY"]]

    (comment
      (let [xf        (comp
                        (xf/mapcat-zset-transaction-xf)
                        (let [pred-1 #(= (:team/name %) "A")
                              pred-2 #(= (:team/color %) "red")
                              pred-3 #(:player/team %)
                              pred-4 #(= (-> % first :team/name) "A")
                              pred-5 #(:player/city %)
                              pred-6 #(-> % first :player/team)
                              pred-7 #(= (:city/name %) "NY")
                              pred-8 #(-> % first :player/city)]
                          (comp
                            ;ignore datoms irrelevant to the query
                            (map (fn [zset]
                                   (xf/disj-irrelevant-items
                                     zset pred-1 pred-2 pred-3 pred-4 pred-5 pred-6 pred-7 pred-8)))
                            (map (fn [tx-current-item] (timbre/spy tx-current-item)))
                            (xf/join-xf
                              pred-1 :db/id
                              pred-2 :db/id
                              index-state-all)
                            (map (fn [zset-in-between] (timbre/spy zset-in-between)))
                            (xf/join-xf
                              pred-3 :player/team
                              pred-4 #(-> % first :db/id)
                              index-state-all)
                            (map (fn [zset-in-between-2] (timbre/spy zset-in-between-2)))
                            (xf/join-xf
                              pred-5 :db/id
                              pred-6 #(-> % first :db/id)
                              index-state-all)
                            (map (fn [zset-in-between-3] (timbre/spy zset-in-between-3)))
                            (xf/join-xf
                              pred-7 :db/id
                              pred-8 #(-> % first :player/city)
                              index-state-all
                              :last? true)
                            (xforms/reduce zs/zset+))))
            output-ch (a/chan (a/sliding-buffer 1)
                        (xf/query-result-set-xf result-set))
            to        (a/pipeline 1 output-ch xf @input)]
        @input))


    ;with vector-datoms
    '[:find ?p                                              ;find is wip
      :where
      [?p :player/team ?t]
      [?t :team/name "A"]
      [?t :team/color "red"]]

    (comment
      (let [xf        (comp
                        (xf/mapcat-zset-transaction-xf)
                        (let [pred-1 #(and (= (datom->attr %) :team/name) (= (datom->val %) "A"))
                              pred-2 #(and (= (datom->attr %) :team/color) (= (datom->val %) "red"))
                              pred-3 #(= (datom->attr %) :player/team)
                              pred-4 #(and (= (-> % first datom->attr) :team/name) (= (-> % first datom->val) "A"))]
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
                              pred-3 datom->val
                              pred-4 #(-> % first datom->eid)
                              index-state-all
                              :last? true)
                            (map (fn [zset-in-between-2] (timbre/spy zset-in-between-2)))
                            ;TODO track :find variables
                            (xforms/reduce zs/zset+))))
            output-ch (a/chan (a/sliding-buffer 1)
                        (xf/query-result-set-xf result-set))
            to        (a/pipeline 1 output-ch xf @input)]
        @input))

    (comment
      (a/>!!
        @input
        [(tx-datoms->zset-2
           [[1 :team/name "A" 0 true]
            [1 :team/color "red" 0 true]])
         (tx-datoms->zset-2
           [[2 :player/team 1 0 true]])])

      (a/>!!
        @input
        [(tx-datoms->zset-2
           [[1 :team/name "A" 0 false]
            [1 :team/color "red" 0 false]])
         (tx-datoms->zset-2
           [[2 :player/team 1 0 false]])]))


    ; equivalent query
    '[:find ?p                                              ;find is wip
      :where
      [?p :person/name ?v]
      [?p :person/friend ?p2]
      [?p2 :person/name ?v2]]

    '[[?p :person/name ?v] [?p :person/friend ?p2]]
    '[?p :person/friend [?p2 :person/name ?v2]]

    ; equivalent query
    '[:find ?p                                              ;find is wip
      :where
      [?p :person/name ?v]
      [?p :person/friend ?p2]
      [?p2 :person/name ?v]]

    '[[?p :person/name ?v] [?p :person/friend ?p2]]
    '[?p :person/friend [?p2 :person/name ?v]]
    '[[?p :person/name ?v] [?p2 :person/name ?v]]


    (comment
      (let [xf        (comp
                        (xf/mapcat-zset-transaction-xf)
                        (let [pred-1 #(datom-attr= % :person/name)
                              pred-2 #(datom-attr= % :person/friend)
                              pred-3 #(datom-attr= (second %) :person/friend)
                              pred-4 #(datom-attr= % :person/name)
                              ]
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
                              :last? true)
                            (map (fn [zset-in-between-last] (timbre/spy zset-in-between-last)))
                            ;TODO explore where filters
                            (xforms/reduce zs/zset+))))
            output-ch (a/chan (a/sliding-buffer 1)
                        (xf/query-result-set-xf result-set))
            to        (a/pipeline 1 output-ch xf @input)]
        @input))

    (comment
      (a/>!!
        @input
        [(tx-datoms->zset-2
           [[1 :person/name "Alice" :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset-2
           [[2 :person/name "Bob" :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset-2
           [[1 :person/friend 2 :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset-2
           [[1 :person/friend 1 :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset-2
           [[3 :person/name "Clark" :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset-2
           [[1 :person/friend 3 :t true]])])

      (a/>!!
        @input
        [(tx-datoms->zset-2
           [[1 :person/name "Alice" :t true]
            [2 :person/name "Bob" :t true]
            [1 :person/friend 2 :t true]])]))

    ;query
    '[:find ?name
     :where
     [?p :person/name ?name]
     [?m :movie/title "RoboCop"]
     [?m :movie/director ?p]]

    (comment
      #{
        [
         ;[?p :person/name ?name] -> [?m :movie/director ?p], joining them returns a joined pair like:
         [[1 :person/name "Alice"]
          [2 :movie/director 1]]

         ;[?m :movie/director ?p] -> ^^^^^^^^^^^^^^^^^^^^^^^ pred-3 looking for the second item from the joined pair)
         [2 :movie/title "RoboCop"]
         ]}
      )

    (let [xf        (comp
                      (xf/mapcat-zset-transaction-xf)
                      (let [pred-1 #(datom-attr= % :person/name)
                            pred-2 #(datom-attr= % :movie/director)
                            pred-3 #(datom-attr= (second %) :movie/director)
                            pred-4 #(datom-attr-val= % :movie/title "RoboCop")
                            ]
                        (comp
                          ;ignore datoms irrelevant to the query
                          (map (fn [zset]
                                 (xf/disj-irrelevant-items
                                   zset pred-1 pred-2 pred-3 pred-4)))
                          (map (fn [tx-current-item] (timbre/spy tx-current-item)))
                          (xf/join-xf
                            pred-1 datom->eid
                            pred-2 datom->val
                            index-state-all)
                          (map (fn [zset-in-between] (timbre/spy zset-in-between)))
                          (xf/join-xf
                            pred-3 #(-> % second datom->eid)
                            pred-4 datom->eid
                            index-state-all
                            :last? true)
                          (map (fn [zset-in-between-last] (timbre/spy zset-in-between-last)))
                          ;TODO explore where filters
                          (xforms/reduce zs/zset+))))
          output-ch (a/chan (a/sliding-buffer 1)
                      (xf/query-result-set-xf result-set))
          to        (a/pipeline 1 output-ch xf @input)]
      @input)

    (a/>!!
      @input
      [(tx-datoms->zset-2
         [[1 :person/name "Alice" :t true]])])

    (a/>!!
      @input
      [(tx-datoms->zset-2
         [[2 :movie/director 1 :t true]])])

    (a/>!!
      @input
      [(tx-datoms->zset-2
         [[2 :movie/title "RoboCop" :t true]])])

    )




  (a/>!!
    @input
    [(tx-datoms->zset
       [[1 :team/name "A" 0 true]
        [1 :team/color "red" 0 true]])
     (tx-datoms->zset
       [[3 :city/name "NY" 0 true]])
     (tx-datoms->zset
       [[2 :player/team 1 0 true]
        [2 :player/city 3 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[2 :player/city 3 0 false]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[2 :player/city 3 0 true]])])


  (a/>!!
    @input
    [(tx-datoms->zset
       [[1 :team/name "A" 0 false]
        [1 :team/color "red" 0 false]])
     (tx-datoms->zset
       [[2 :player/team 1 0 false]
        [2 :player/city 3 0 false]])
     (tx-datoms->zset
       [[3 :city/name "NY" 0 false]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[3 :team/name "A" 0 true]
        [3 :team/color "red" 0 true]])
     (tx-datoms->zset
       [[4 :player/team 3 0 true]
        [4 :player/city "NY" 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[3 :team/name "A" 0 false]
        [3 :team/color "red" 0 false]])
     (tx-datoms->zset
       [[4 :player/team 3 0 false]
        [4 :player/city "NY" 0 false]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[4 :player/team 1 0 true]
        [4 :player/city "NY" 0 true]])])

  (a/>!!
    @input
    [(zs/zset-negate
       (tx-datoms->zset
         [[3 :team/name "B" 0 true]]))
     (tx-datoms->zset
       [[3 :team/name "A" 0 true]])])

  (a/>!!
    @input
    [(zs/zset-negate
       (tx-datoms->zset
         [[1 :team/name "A" 0 true]
          [1 :team/color "red" 0 true]]))
     (zs/zset-negate
       (tx-datoms->zset
         [[2 :player/team 1 0 true]
          [2 :player/city "NY" 0 true]]))])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[1 :team/name "A" 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[1 :team/color "red" 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[2 :player/team 1 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[2 :player/city "NY" 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[3 :player/team 1 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[11 :team/name "A" 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[11 :team/color "red" 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[11 :team/color "red" 0 false]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[4242 :abc "..." 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[3 :player/team 11 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[11 :team/color "red" 0 false]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[1 :team/color "red" 0 false]])])
  )
