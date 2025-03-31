(ns org.zsxf.datascript
  (:require [org.zsxf.zset :as zs])
  (:import (clojure.lang Associative IHashEq ILookup IObj IPersistentCollection Indexed Seqable)
           (datascript.db Datom IDatom)))

(deftype Datom2 [^Datom datom meta]

  ;Extends Datascript datoms to support metadata, and potentially more in the future)
  ; (!) Clojure-only at the moment, ClojureScript requires a slightly different set of methods.
  ; This will potentially allow lower memory usage (TBD) as the Datom objects
  ; are referenced and boxed directly inside Datom2 instead of being converted to vectors each time.
  ; Datom2 almost entirely calls the Datascript Datom type methods directly to preserve
  ; exact behavior, apart from the addition of IObj to support metadata

  ;New!
  IObj
  (meta [_] meta)
  (withMeta [_ m] (Datom2. datom m))
  ;All below almost directly pass through call execution to datascript.db.Datom
  IDatom
  (datom-tx [_] (.datom-tx datom))
  (datom-added [_] (.datom-added datom))
  (datom-get-idx [_] (.datom-get-idx datom))
  (datom-set-idx [_ value] (.datom-set-idx datom value))
  IObj
  (meta [_] meta)
  (withMeta [_ m] (Datom2. datom m))
  Object
  (hashCode [self]
    (.hashCode datom))
  (toString [self] (.toString datom))
  IHashEq
  (hasheq [self] (.hasheq datom))
  Seqable
  (seq [self] (.seq datom))
  IPersistentCollection
  (equiv [self x]
    ;small difference in this one:
    ;check if x is Datom2, if yes, "unwrap" it and pass through
    (cond
      (instance? Datom2 x) (.equiv datom (.-datom ^Datom2 x)) ;unwrap
      :else (.equiv datom x)))                              ;in any other case, call through direct
  (empty [self] (throw (UnsupportedOperationException. "empty is not supported on Datom")))
  (count [self] 5)
  (cons [self v] (.cons datom v))
  ;
  Indexed
  (nth [self i] (.nth datom i))
  (nth [self i not-found] (.nth datom i not-found))
  ;
  ILookup
  (valAt [self k] (.valAt datom k))
  (valAt [self k nf] (.valAt datom k nf))
  ;
  Associative
  (entryAt [self k] (.entryAt datom k))
  (containsKey [self k] (.containsKey datom k))
  (assoc [self k v] (.assoc datom k v)))

(defn datom2 [datom]
  (->Datom2 datom nil))

(comment
  (=
    (with-meta
      (datom2 (d/datom 1 :a "v"))
      {:mmmm 42})
    (with-meta
      (datom2 (d/datom 1 :a "v"))
      {:mmmm 43})))


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
