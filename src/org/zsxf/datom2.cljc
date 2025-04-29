(ns org.zsxf.datom2
  (:require [org.zsxf.util :as util]
            #?(:clj [org.zsxf.type :as t])
            [org.zsxf.zset :as zs])
  #?(:clj
     (:import (org.zsxf.type Datom2))))


(defn datom2
  ([datascript-datom]
   (datom2 datascript-datom nil))
  ([datascript-datom metadata]
   #?(:clj
      (t/->Datom2 datascript-datom metadata)
      :cljs
      ;TODO implement Datom2 for CLJS
      (let [[e a v _t add-or-retract :as datom] datascript-datom]
        [e a v]))))

(defn datom2? [x]
  #?(:clj
     (instance? Datom2 x)
     :cljs
     ;TODO implement Datom2 for CLJS
     (util/datom-like? x)))

(defn datom->weight [datom]
  (let [weight (condp = (nth datom 4) true 1 false -1)]
    weight))

(defn datom->datom2->zset-item [datom]
  (zs/zset-item (datom2 datom) (datom->weight datom)))

(defn datom->eid [datom]
  (when (datom2? datom)
    (nth datom 0 nil)))

(defn datom->attr [datom]
  (when (datom2? datom)
    (nth datom 1 nil)))

(defn datom-attr= [datom attr]
  (= (datom->attr datom) attr))

(defn datom->val [datom]
  (when (datom2? datom)
    (nth datom 2 nil)))

(defn ?datom->val [datom]
  (if (datom2? datom)
    (nth datom 2 nil)
    datom))

(defn datom-val= [datom value]
  (= (datom->val datom) value))

(defn datom-attr-val= [datom attr value]
  (and (datom-attr= datom attr) (datom-val= datom value)))

(defn tx-datoms->datoms2->zset
  "Transforms datoms into a zset of vectors. Each vector represents a datom with a weight."
  [datoms]
  (transduce
    (map datom->datom2->zset-item)
    conj
    #{}
    datoms))

(defn tx-datoms->datoms2->zsets
  "Transforms datoms into datoms2, and then into a vector of zsets.
  Useful to maintain inter-transaction order of datoms."
  [datoms]
  (into
    []
    (comp
      (map datom->datom2->zset-item)
      (map hash-set))
    datoms))
