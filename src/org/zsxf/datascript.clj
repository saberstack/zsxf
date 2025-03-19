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

  (let [xf      (comp
                  (xf/mapcat-zset-tx)
                  (xf/join-xf
                    #(= (:team/name %) "A") :db/id index-state-1
                    #(= (:team/color %) "red") :db/id index-state-2)
                  (xf/join-xf
                    #(:player/team %) :player/team index-state-3
                    #(= (-> % first :team/name) "A") #(-> % first :db/id) index-state-4))
        tx-data [(tx-datoms->zset
                   [[1 :team/name "A" 0 true]
                    ;[1 :team/color "red" 0 true]
                    ])
                 #_(tx-datoms->zset
                     [[2 :player/name "Alice" 0 true]
                      [2 :player/team 1 0 true]
                      [2 :player/city "NY" 0 true]])
                 #_(tx-datoms->zset
                     [[1 :team/color "red" 0 true]])]]
    (let [output-ch (a/chan (a/sliding-buffer 1)
                      (map (fn [final-delta] (timbre/spy final-delta))))
          to        (a/pipeline 1 output-ch xf @input)]
      (a/>!! @input tx-data)))

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
    input
    [(tx-datoms->zset
       [[1 :team/color "red" 0 false]])])
  )
