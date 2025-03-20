(ns org.zsxf.datascript
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.zset :as zset]
            [org.zsxf.xf :as xf]
            [datascript.core :as d]
            [pangloss.transducers :as pxf]
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

        [?p :player/team [?t :team/name "T"]0])

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
  nil)

(comment

  (print-index-state)

  (d/q
    '[:find ?p                                            ;wip
      :where
      [?p :player/team ?t]
      [?t :team/name "A"]
      [?t :team/color "red"]]
    @conn)

  (do (reset! index-state-1 {})
    (reset! index-state-2 {})
    (reset! index-state-3 {})
    (reset! index-state-4 {})
    (reset! result-set #{})
    (reset! input (a/chan))

    (let [xf (comp
               (xf/mapcat-zset-transaction-xf)
               (let [pred-1 #(= (:team/name %) "A")
                     pred-2 #(= (:team/color %) "red")
                     pred-3 #(:player/team %)
                     pred-4 #(= (-> % first :team/name) "A")]
                 (comp
                   ;ignore datoms irrelevant to the query
                   (filter #(some (some-fn pred-1 pred-2 pred-3 pred-4) %))
                   (map (fn [tx-current-item] (timbre/spy tx-current-item)))
                   (xf/join-xf
                     pred-1 :db/id index-state-1
                     pred-2 :db/id index-state-2)
                   (map (fn [zset-in-between] (timbre/spy zset-in-between)))
                   (xf/join-xf
                     pred-3 :player/team index-state-3
                     pred-4 #(-> % first :db/id) index-state-4)
                   (xforms/reduce zs/zset+))))]
      (let [output-ch (a/chan (a/sliding-buffer 1)
                        (xf/query-result-set-xf result-set))
            to        (a/pipeline 1 output-ch xf @input)]
        @input)))

  (a/>!!
    @input
    [(tx-datoms->zset
       [[1 :team/name "A" 0 true]
        [1 :team/color "red" 0 true]])

     (tx-datoms->zset
       [[2 :player/team  1 0 true]])])

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
       [[2 :player/team  1 0 true]])])

  (a/>!!
    @input
    [(tx-datoms->zset
       [[3 :player/team  1 0 true]])])

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
