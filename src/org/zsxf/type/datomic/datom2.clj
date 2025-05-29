(ns org.zsxf.type.datomic.datom2
  (:import (clojure.lang Counted IHashEq ILookup IObj IPersistentCollection Indexed)
           (datomic.db Datum)
           (java.io Writer)))


(deftype DatomicDatom2 [^Datum datom meta]
  IObj
  (meta [this] meta)
  (withMeta [this m] (DatomicDatom2. datom m))

  Object
  (hashCode [self] (.hashCode datom))
  (toString [self] (.toString datom))

  Indexed
  (nth [this i] (.nth datom i))
  (nth [this i not-found] (.nth datom i not-found))

  ILookup
  (valAt [this k] (.valAt datom k))
  (valAt [this k nf] (.valAt datom k nf))

  Counted
  (count [this] 5)

  IPersistentCollection
  (equiv [self x]
    (cond
      (instance? DatomicDatom2 x)
      (= datom (.-datom ^DatomicDatom2 x)) ;unwrap
      :else (= datom x)))

  )

(defn ddatom2 [datomic-datom]
  (->DatomicDatom2 datomic-datom nil))


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

(defmethod print-method DatomicDatom2 [^DatomicDatom2 ddatom2, ^Writer w]
  (binding [*out* w]
    (let [^Datum d (.-datom ddatom2)]
      (print-meta ddatom2 w)
      (.write w "#dd2")
      (pr [(.-e d) (.-a d) (.-v d)]))))
