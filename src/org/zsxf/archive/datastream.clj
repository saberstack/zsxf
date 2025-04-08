(ns org.zsxf.archive.datastream
  (:require [org.zsxf.zset :as zset]
            [datascript.core :as d]))

(defn trim-tuples
  "Datoms can have a fourth element to the tuple, which is a boolean:
    - true for an addition
    - false for a retraction
  As we convert to zsets, this boolean becomes a weight of 1 or -1, respectively.
  Once encoded as zset weight, this value should not remain in the tuples,
  or it will mess with zset composition later on."
  [datom-tuples]
  (map (partial take 3) datom-tuples))

(defn tx->zset
  "Convert a sequence of datascript Datoms into a zset,
  with positive weights for additions and negative for retractions."
  [datoms]
  (let [datoms' (map seq datoms)
        {additions true retractions false}
        (group-by #(nth % 4) datoms')]
    (zset/zset+ (zset/zset (trim-tuples additions))
                (zset/zset-negative (trim-tuples retractions)))))

(defn db->stream-of-changes
  "Get all of the data out of the eavt index in the database.
  Is a transducer that emits a zset for every transaction in the database.

  Note that datascript does not retain retracted datoms, so only data
  still in the database will come through this stream."
  [db]
  (transduce
     (comp
      (partition-by (fn [[e a v tx added? :as datom]] tx))
      (map tx->zset))
     conj
     []
     (:eavt db)))

(defn listen-datom-stream
  "Takes a datascript db connection and an atom.
  Whenever the database is transacted with via transact!, a zset
  representing the transaction will be conj'd into the atom.

  Note that other ways of interacting with the database, such as db-with,
  will evade this listening mechanism."
  ([conn atom]
   (listen-datom-stream conn atom tx->zset))
  ([conn atom tx-data-f]
   (d/listen! conn (fn [tx-report]
                     (swap! atom conj
                       (tx-data-f (:tx-data tx-report)))))))
