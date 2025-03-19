(ns org.zsxf.datascript
  (:require [clojure.core.async :as a]
            [org.zsxf.zset :as zset]
            [org.zsxf.xf :as xf]
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

(comment

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

(defonce input (atom (a/chan)))

(comment

  (do (reset! index-state-1 {})
    (reset! index-state-2 {})
    (reset! index-state-3 {})
    (reset! index-state-4 {})
    (reset! input (a/chan)))

  (let [xf (comp
             (xf/mapcat-zset-tx)
             (let [pred-1 #(= (:team/name %) "A")
                   pred-2 #(= (:team/color %) "red")
                   pred-3 #(:player/team %)
                   pred-4 #(= (-> % first :team/name) "A")]
               (comp
                 ;ignore datoms irrelevant to the query
                 (filter #(some (some-fn pred-1 pred-2 pred-3 pred-4) %))
                 (xf/join-xf
                   pred-1 :db/id index-state-1
                   pred-2 :db/id index-state-2)
                 (xf/join-xf
                   pred-3 :player/team index-state-3
                   pred-4 #(-> % first :db/id) index-state-4))))]
    (let [output-ch (a/chan (a/sliding-buffer 1)
                      (map (fn [final-delta] (timbre/spy final-delta))))
          to        (a/pipeline 1 output-ch xf @input)]
      @input))

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
