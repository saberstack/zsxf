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

(defn datom->val [datom]
  (if (vector? datom)
    (nth datom 2 nil)))

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

(defonce index-state-1 (atom {}))
(defonce index-state-2 (atom {}))
(defonce index-state-3 (atom {}))
(defonce index-state-4 (atom {}))
(defonce index-state-5 (atom {}))
(defonce index-state-6 (atom {}))
(defonce index-state-7 (atom {}))
(defonce index-state-8 (atom {}))
(defonce result-set (atom #{}))

(comment
  ;transform result set to datascript form
  (into #{}
    (map (fn [result-set-item] [(:db/id (first result-set-item))]))
    @result-set))

(defonce input (atom (a/chan)))

(defn print-index-state []
  (timbre/spy @index-state-1)
  (timbre/spy @index-state-2)
  (timbre/spy @index-state-3)
  (timbre/spy @index-state-4)
  (timbre/spy @index-state-5)
  (timbre/spy @index-state-6)
  (timbre/spy @index-state-7)
  (timbre/spy @index-state-8)
  nil)

(comment

  (print-index-state)

  (do
    (reset! index-state-1 {})
    (reset! index-state-2 {})
    (reset! index-state-3 {})
    (reset! index-state-4 {})
    (reset! index-state-5 {})
    (reset! index-state-6 {})
    (reset! index-state-7 {})
    (reset! index-state-8 {})
    (reset! result-set #{})
    (reset! input (a/chan))

    ;with map-datoms
    ;equivalent query
    '[:find ?p                                              ;find is wip
      :where
      [?p :player/team ?t]
      [?t :team/name "A"]
      [?t :team/color "red"]
      [?p :player/city ?c]
      [?c :city/name "NY"]]
    ;
    #_(let [xf        (comp
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
                              pred-1 :db/id index-state-1
                              pred-2 :db/id index-state-2)
                            (map (fn [zset-in-between] (timbre/spy zset-in-between)))
                            (xf/join-xf
                              pred-3 :player/team index-state-3
                              pred-4 #(-> % first :db/id) index-state-4)
                            (map (fn [zset-in-between-2] (timbre/spy zset-in-between-2)))
                            (xf/join-xf
                              pred-5 :db/id index-state-5
                              pred-6 #(-> % first :db/id) index-state-6)
                            (map (fn [zset-in-between-3] (timbre/spy zset-in-between-3)))
                            (xf/join-xf
                              pred-7 :db/id index-state-7
                              pred-8 #(-> % first :player/city) index-state-8)
                            (xforms/reduce zs/zset+))))
            output-ch (a/chan (a/sliding-buffer 1)
                        (xf/query-result-set-xf result-set))
            to        (a/pipeline 1 output-ch xf @input)]
        @input)

    ;with vector-datoms
    '[:find ?p                                              ;find is wip
      :where
      [?p :player/team ?t]
      [?t :team/name "A"]
      [?t :team/color "red"]]

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
                            pred-1 datom->eid index-state-1
                            pred-2 datom->eid index-state-2)
                          (map (fn [zset-in-between] (timbre/spy zset-in-between)))
                          (xf/join-xf
                            pred-3 datom->val index-state-3
                            pred-4 #(-> % first datom->eid) index-state-4)
                          (map (fn [zset-in-between-2] (timbre/spy zset-in-between-2)))
                          (xforms/reduce zs/zset+))))
          output-ch (a/chan (a/sliding-buffer 1)
                      (xf/query-result-set-xf result-set))
          to        (a/pipeline 1 output-ch xf @input)]
      @input))

  (a/>!!
    @input
    [(tx-datoms->zset-2
       [[1 :team/name "A" 0 true]
        [1 :team/color "red" 0 true]])
     #_(tx-datoms->zset-2
         [[3 :city/name "NY" 0 true]])
     (tx-datoms->zset-2
       [[2 :player/team 1 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset-2
       [[1 :team/name "A" 0 false]
        [1 :team/color "red" 0 false]])
     (tx-datoms->zset-2
       [[2 :player/team 1 0 false]])])

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
