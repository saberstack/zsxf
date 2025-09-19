(ns org.zsxf.type.datascript.datom2
  (:require [datascript.core :as d]
            [datascript.db :as ddb]
            [org.zsxf.type.datom-like :as dl])
  (:import (clojure.lang Associative IHashEq ILookup IObj IPersistentCollection Indexed Seqable)
           (datascript.db Datom)
           (java.io Writer)))

(deftype Datom2 [^Datom datom meta]
  ;marker protocol
  dl/DatomLike
  (-datom-like? [this] true)

  ;Extends Datascript datoms to support metadata, and potentially more features in the future.
  ; (!) Clojure-only at the moment, ClojureScript requires a slightly different set of methods.
  ; This allows lower memory usage as the Datom objects
  ; are referenced and boxed directly inside Datom2 instead of being converted to vectors each time.
  ; Datom2 almost entirely calls the Datascript Datom type methods directly to preserve
  ; exact behavior, apart from the addition of IObj to support metadata

  ;New!
  IObj
  (meta [this] meta)
  (withMeta [this m] (Datom2. datom m))

  ;All below almost directly pass through call execution to datascript.db.Datom
  ddb/IDatom
  (datom-tx [this] (ddb/datom-tx datom))
  (datom-added [this] (ddb/datom-added datom))
  (datom-get-idx [this] (ddb/datom-get-idx datom))
  (datom-set-idx [this value] (ddb/datom-set-idx datom value))

  Object
  (hashCode [this] (.hashCode datom))
  (toString [this] (.toString datom))
  (equals [this x]
    (or (identical? this x)
      (= this x)))

  IHashEq
  (hasheq [this] (.hasheq datom))

  Seqable
  (seq [this] (.seq datom))

  IPersistentCollection
  (equiv [this x]
    ;WARNING about (= ...)
    ; Mixing Datom and Datom2 (unlikely) can output the wrong result:
    ;
    ;(=
    ; (datom2 (d/datom 1 :a "v"))
    ; (d/datom 1 :a "v"))
    ;;=> true ;looks good!
    ;
    ; ... but this one is wrong!
    ;
    ;(=
    ;  (datom2 (d/datom 1 :a "v"))
    ;  (d/datom 1 :a "v")
    ;  (datom2 (d/datom 1 :a "v")))
    ;;=> false
    ;
    ; This is because (= ...) compares items in overlapping pairs,
    ; so in the latter case once it reaches the second item it will defer
    ; the equiv decision to Datascript's Datom deftype which has a strict type check

    ;check if x is Datom2, if yes, "unwrap" it and pass through
    (cond
      (instance? Datom2 x) (.equiv datom (.-datom ^Datom2 x)) ;unwrap
      :else (.equiv datom x)))
  (empty [this] (throw (UnsupportedOperationException. "empty is not supported on Datom")))
  (count [this] 5)
  (cons [this v] (.cons datom v))

  Indexed
  (nth [this i] (.nth datom i))
  (nth [this i not-found] (.nth datom i not-found))

  ILookup
  (valAt [this k] (.valAt datom k))
  (valAt [this k nf] (.valAt datom k nf))

  Associative
  (entryAt [this k] (.entryAt datom k))
  (containsKey [this k] (.containsKey datom k))
  (assoc [this k v] (.assoc datom k v)))

(comment
  ;BEWARE of deftype with the REPL...

  ;eval those first
  (defonce datom2-1 (->Datom2 (d/datom 1 :a "42") nil))
  (defonce datom2-2 (->Datom2 (d/datom 1 :a "42") nil))
  ;equal? yes, of course
  (= datom2-1 datom2-2)
  ;=> true

  ;now... reload the deftype...
  (defonce datom2-3 (->Datom2 (d/datom 1 :a "42") nil))
  (defonce datom2-4 (->Datom2 (d/datom 1 :a "42") nil))
  ;3 and 4 are still equal
  (= datom2-1 datom2-2)
  ;=> true
  ;
  ;HOWEVER... 2 and 3...
  (= datom2-2 datom2-3)
  ;=> false (!!!)
  )


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
      (.write w "#d2")
      (pr [(.-e d) (.-a d) (.-v d)]))))

;New print
;(defn- print-meta-arg [o, ^Writer w]
;  (when-let [m (meta o)]
;    (when (and (pos? (count m))
;            (or *print-dup*
;              (and *print-meta* *print-readably*)))
;      (.write w " ")
;      (if (and (= (count m) 1) (:tag m))
;        (pr-on (:tag m) w)
;        (pr-on m w))
;      (.write w " "))))
;
;(defmethod print-method Datom2 [^Datom2 datom2, ^Writer w]
;  (binding [*out* w]
;    (let [^Datom d (.-datom datom2)]
;      (.write w "(org.zsxf.datom2/datom2 (datascript.core/datom ")
;      (pr (.-e d) (.-a d) (.-v d))
;      (.write w ")")
;      (print-meta-arg datom2 w)
;      (.write w ")"))))

(defn datom-from-reader [v]
  ; This does not seem possible until this is solved:
  ; https://clojure.atlassian.net/jira/software/c/projects/CLJ/issues/CLJ-2904
  `(->Datom2 (ddb/datom ~@v) nil))

;; Custom printing end
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
