(ns org.zsxf.datom
  (:require
   [org.zsxf.type.datom-like :as dl]
   [org.zsxf.util :as util]
   #?(:clj [org.zsxf.type.datascript.datom2 :as td])
   #?(:clj [org.zsxf.type.datomic.datom2 :as dd2])
   [org.zsxf.zset :as zs])
  #?(:clj
     (:import (org.zsxf.type.datascript.datom2 Datom2))))

(defn datom2
  "Takes a datom, i.e. (datascript.core/datom 1 :a 'v) and returns a Datom2"
  ([datom]
   (datom2 datom nil))
  ([datom metadata]
   #?(:clj
      (td/->Datom2 datom metadata)
      :cljs
      ;TODO implement Datom2 for CLJS
      (let [[e a v _t add-or-retract :as datom] datom]
        [e a v]))))

(comment
  ;; Used sometimes for hand testing
  (defn tuple->datom2
   ([[e a v]]
    (datom2 (datascript.core/datom e a v)))
   ([e a v]
    (datom2 (datascript.core/datom e a v)))))

(defn ds-datom->datom2->zset-item [datom]
  (zs/zset-item (datom2 datom) (zs/bool->weight (nth datom 4))))

(defn datom->eid [datom]
  (when (dl/datom-like? datom)
    (nth datom 0 nil)))

(defn datom->attr [datom]
  (when (dl/datom-like? datom)
    (nth datom 1 nil)))

(defn datom-attr= [datom attr]
  (= (datom->attr datom) attr))

(defn datom-eid= [datom eid]
  (= (datom->eid datom) eid))

(defn datom->val [datom]
  (when (dl/datom-like? datom)
    (nth datom 2 nil)))

(defn datom-val= [datom value]
  (= (datom->val datom) value))

(defn datom-attr-val= [datom attr value]
  (and (datom-attr= datom attr) (datom-val= datom value)))

(defn ds-tx-datoms->datoms2->zsets
  "Transforms datoms into datoms2, and then into a vector of zsets.
  Useful to maintain inter-transaction order of datoms."
  [datoms]
  (into
    []
    (comp
      (map ds-datom->datom2->zset-item)
      (map hash-set))
    datoms))
