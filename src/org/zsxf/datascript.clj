(ns org.zsxf.datascript
  (:require [org.zsxf.zset :as zset]
            [org.zsxf.xf :as xf]))


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
  (comment
    (let [index-state-1 (atom {})
          index-state-2 (atom {})]
      (into []
        (comp
          (xf/mapcat-zset-tx)
          (xf/join-xf
            :player/team :player/team index-state-1
            (fn [m] (and (= (:team/name m)) (= (:team/name m) "A"))) :db/id index-state-2))
        [[(tx-datoms->zset
            [[1 :team/name "A" 0 true]])
          (tx-datoms->zset
            [[2 :player/name "Alice" 0 true]
             [2 :player/team 1 0 true]])]]))))
