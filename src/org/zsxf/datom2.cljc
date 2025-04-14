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

#?(:cljs
   (defn datom-like?
     ;TODO remove this once there's Datom2 for CLJS
     [x]
     (boolean
       (and (util/nth2 x 0) (util/nth2 x 1) (util/nth2 x 2)
         (int? (util/nth2 x 0))
         (keyword? (util/nth2 x 1))))))

(defn datom? [x]
  #?(:clj
     (instance? Datom2 x)
     :cljs
     ;TODO implement Datom2 for CLJS
     (datom-like? x)))

(defn datom->weight [datom]
  (let [weight (condp = (nth datom 4) true 1 false -1)]
    weight))

(defn datom->datom2->zset-item [datom]
  (zs/zset-item (datom2 datom) (datom->weight datom)))

(defn datom->eid [datom]
  (if (datom? datom)
    (nth datom 0 nil)))

(defn datom->attr [datom]
  (if (datom? datom)
    (nth datom 1 nil)))

(defn datom-attr= [datom attr]
  (= (datom->attr datom) attr))

(defn datom->val [datom]
  (if (datom? datom)
    (nth datom 2 nil)))

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
