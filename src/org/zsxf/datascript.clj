(ns org.zsxf.datascript
  (:require [org.zsxf.zset :as zs]
            [datascript.core :as d]
            [datascript.db :as ddb]
            [taoensso.timbre :as timbre])
  (:import (clojure.lang Associative IHashEq ILookup IObj IPersistentCollection Indexed Seqable)
           (datascript.db Datom)
           (java.io Writer)))

(deftype Datom2 [^Datom datom meta]

  ;Extends Datascript datoms to support metadata, and potentially more features in the future.
  ; (!) Clojure-only at the moment, ClojureScript requires a slightly different set of methods.
  ; This will potentially allow lower memory usage (TBD) as the Datom objects
  ; are referenced and boxed directly inside Datom2 instead of being converted to vectors each time.
  ; Datom2 almost entirely calls the Datascript Datom type methods directly to preserve
  ; exact behavior, apart from the addition of IObj to support metadata

  ;New!
  IObj
  (meta [self] meta)
  (withMeta [self m] (Datom2. datom m))

  ;All below almost directly pass through call execution to datascript.db.Datom
  ddb/IDatom
  (datom-tx [self] (ddb/datom-tx datom))
  (datom-added [self] (ddb/datom-added datom))
  (datom-get-idx [self] (ddb/datom-get-idx datom))
  (datom-set-idx [self value] (ddb/datom-set-idx datom value))

  Object
  (hashCode [self] (.hashCode datom))
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

  Indexed
  (nth [self i] (.nth datom i))
  (nth [self i not-found] (.nth datom i not-found))

  ILookup
  (valAt [self k] (.valAt datom k))
  (valAt [self k nf] (.valAt datom k nf))

  Associative
  (entryAt [self k] (.entryAt datom k))
  (containsKey [self k] (.containsKey datom k))
  (assoc [self k v] (.assoc datom k v)))

(defn datom2 [datom]
  (->Datom2 datom nil))

(defn datom-from-reader [v]
  (datom2 (apply ddb/datom v)))

;; Custom printing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn pr-on
  [x w]
  (if *print-dup*
    (print-dup x w)
    (print-method x w))
  nil)

(defn- print-meta [o, ^Writer w]
  (when-let [m (meta o)]
    (when (and (pos? (count m))
            (or *print-dup*
              (and *print-meta* *print-readably*)))
      (.write w " ^")
      (if (and (= (count m) 1) (:tag m))
        (pr-on (:tag m) w)
        (pr-on m w))
      (.write w " "))))

(defmethod print-method Datom2 [^Datom2 datom2, ^Writer w]
  (binding [*out* w]
    (let [^Datom d (.-datom datom2)]
      (print-meta datom2 w)
      (.write w "#org.zsxf.datascript/Datom2")
      (pr [(.-e d) (.-a d) (.-v d) (ddb/datom-tx d) (ddb/datom-added d)]))))

;; Custom printing end
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(comment
  (set! *print-meta* true)
  (=
    (with-meta
      (datom2 (d/datom 1 :a "v"))
      {:mmmm 42})
    (with-meta
      (datom2 (d/datom 1 :a "v"))
      {:mmmm 43})))

(defn datom->weight [datom]
  (let [weight (condp = (nth datom 4) true 1 false -1)]
    weight))

(defn datom->zset-item [[e a v _t add-or-retract :as datom]]
  (zs/zset-item [e a v] (datom->weight datom)))

(defn datom->datom2->zset-item [datom]
  (zs/zset-item (datom2 datom) (datom->weight datom)))

(defn tx-datoms->zset
  "Transforms datoms into a zset of vectors. Each vector represents a datom with a weight."
  [datoms]
  (transduce
    (map datom->zset-item)
    conj
    #{}
    datoms))

(defn tx-datoms->datoms2->zset
  "Transforms datoms into a zset of vectors. Each vector represents a datom with a weight."
  [datoms]
  (transduce
    (map datom->datom2->zset-item)
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

(defn tx-datoms->zsets2
  "Transforms datoms into datoms2, and then into a vector of zsets.
  Useful to maintain inter-transaction order of datoms."
  [datoms]
  (transduce
    (comp
      (map datom->datom2->zset-item)
      (map hash-set))
    conj
    []
    datoms))

(defn eligible-datom? [datom]
  (or
    (vector? datom)
    (instance? Datom datom)
    (instance? Datom2 datom)))

(defn datom->eid [datom]
  (if (eligible-datom? datom)
    (nth datom 0 nil)))

(defn datom->attr [datom]
  (if (eligible-datom? datom)
    (nth datom 1 nil)))

(defn datom-attr= [datom attr]
  (= (datom->attr datom) attr))

(defn datom->val [datom]
  (if (eligible-datom? datom)
    (nth datom 2 nil)))

(defn datom-val= [datom value]
  (= (datom->val datom) value))

(defn datom-attr-val= [datom attr value]
  (and (datom-attr= datom attr) (datom-val= datom value)))
