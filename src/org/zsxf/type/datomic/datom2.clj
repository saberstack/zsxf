(ns org.zsxf.type.datomic.datom2
  (:require [org.zsxf.type.datom-like :as dl])
  (:import (clojure.lang Counted IHashEq ILookup IObj IPersistentCollection IPersistentMap Indexed Keyword)
           (datomic.db Datum)
           (java.io Writer)))


(deftype DatomicDatom2 [^Datum datom ^Keyword attr ^IPersistentMap metadata]
  ;marker protocol
  dl/DatomLike

  IObj
  (meta [this] metadata)
  (withMeta [this m] (DatomicDatom2. datom attr m))

  Object
  (hashCode [self] (.hashCode datom))
  (toString [self] (.toString datom))

  Indexed
  (nth [this i]
    (if (= i 1) attr (.nth datom i)))
  (nth [this i not-found]
    (if (= i 1) attr (.nth datom i not-found)))

  ILookup
  (valAt [this k]
    (if (= k 1) attr (.valAt datom k)))
  (valAt [this k nf]
    (if (= k 1) attr (.valAt datom k nf)))

  Counted
  (count [this] 5)

  IPersistentCollection
  (equiv [self x]
    (cond
      (instance? DatomicDatom2 x)
      (= datom (.-datom ^DatomicDatom2 x))                  ;unwrap
      :else (= datom x))))

(defn ddatom2 [datomic-datom datom-attr]
  (->DatomicDatom2 datomic-datom datom-attr nil))


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

(defmethod print-method DatomicDatom2 [^DatomicDatom2 datomic-datom2, ^Writer w]
  (binding [*out* w]
    (let [^Datum d (.-datom datomic-datom2)]
      (print-meta datomic-datom2 w)
      (.write w "#dd2")
      (pr [(.-e d) (.-attr datomic-datom2) (.-v d)]))))
