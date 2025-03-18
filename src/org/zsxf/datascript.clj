(ns org.zsxf.datascript
  (:require [org.zsxf.zset :as zset]
            [org.zsxf.xf :as xf]
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

  (let [index-state-1 (atom {})
        index-state-2 (atom {})]
    (into []
      (comp
        (xf/mapcat-zset-tx)
        (xf/join-xf
          ;TODO how do we handle the case when the index k is not present in all cases
          [#(:player/team %)] :player/team index-state-1
          [#(= (:team/name %) "A") #_#(= (:team/color %) "red")] :db/id index-state-2)
        #_(map (fn [zset-delta]
               (into #{}
                 (map (fn [v]
                        (with-meta (vector (-> v first :db/id)) (meta v))))
                 zset-delta))))
      [[(tx-datoms->zset
          [[1 :team/name "A" 0 true]])
        (tx-datoms->zset
          [[2 :player/name "Alice" 0 true]
           [2 :player/team 1 0 true]
           [2 :player/city "NY" 0 true]])
        (tx-datoms->zset
          [[1 :team/color "red" 0 true]])]]))
  )
