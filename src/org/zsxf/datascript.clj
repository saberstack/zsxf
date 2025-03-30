(ns org.zsxf.datascript
  (:require [org.zsxf.zset :as zs]))

(defn datom->zset-item [[e a v _t add-or-retract]]
  (let [weight (condp = add-or-retract true 1 false -1)]
    (zs/zset-item [e a v] weight)))

(defn tx-datoms->zset
  "Transforms datoms into a zset of vectors. Each vector represents a datom with a weight."
  [datoms]
  (transduce
    (map datom->zset-item)
    conj
    #{}
    datoms))

(defn tx-datoms->zsets
  "Transforms datoms into a vector of zsets.
  Useful to maintain inter-transaction order of datoms."
  [datoms]
  (transduce
    (comp
      (map datom->zset-item)
      (map hash-set))
    conj
    []
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

(defn datom->val-meta [datom]
  (with-meta
    (vector (datom->val datom))
    (meta datom)))

(defn datom-val= [datom value]
  (= (datom->val datom) value))

(defn datom-attr-val= [datom attr value]
  (and (datom-attr= datom attr) (datom-val= datom value)))
