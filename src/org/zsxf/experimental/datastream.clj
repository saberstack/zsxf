(ns org.zsxf.experimental.datastream
  (:require [org.zsxf.zset :as zset]))


(defn tx->zset [datoms]
  (let [datoms' (map seq datoms)
        {additions true retractions false}
        (group-by #(nth % 4) datoms')]
    (zset/zset+ (zset/zset additions)
                (zset/zset-negative retractions))))

(defn db->stream-of-changes [db]
  (let [eavt (:eavt db)]
    (transduce
     (comp
      (partition-by (fn [[_ _ _ tx :as datom]] tx))
      (map tx->zset))
     conj
     []
     eavt)))
