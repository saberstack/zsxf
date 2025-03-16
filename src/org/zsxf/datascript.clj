(ns org.zsxf.datascript
  (:require [org.zsxf.zset :as zset]))


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
