(ns org.zsxf.experimental.bifurcan
  (:require [bifurcan-clj.core :as bf]
            [bifurcan-clj.map :as bfm]
            [bifurcan-clj.util]
            [bifurcan-clj.core :as bfc]
            [taoensso.timbre :as timbre])
  (:import (clojure.lang Associative Counted ILookup IPersistentCollection IPersistentMap MapEntry Seqable)
           (io.lacuna.bifurcan IMap Maps$Entry Maps$HashEntry)
           (java.io Writer)))

;(timbre/set-ns-min-level! :info)

(defn- empty-bf-map [] (bfm/map))

(defn- bfm-entry->clj-entry [^Maps$Entry bfm-entry]
  (MapEntry. (.key bfm-entry) (.value bfm-entry)))

(deftype CljBifurcanMap [^IMap bfm]
  IPersistentMap
  (assoc [this k v]
    (timbre/spy "assoc-ing")
    (timbre/spy [k v])
    (CljBifurcanMap. (bfm/put (.-bfm this) k v)))
  (assocEx [this k v] (throw (Exception.)))
  (without [this k]
    (CljBifurcanMap. (bfm/remove (.-bfm this) k)))
  Counted                                                   ;Counted works
  (count [this] (bfc/size (.-bfm this)))
  Iterable
  (iterator [this] (.iterator bfm))
  (spliterator [this])
  (forEach [this each]
    (timbre/spy each)
    nil)
  Associative
  (containsKey [this k]
    #_contains?
    (not= nil (bfm/get bfm k)))
  (entryAt [this k]
    #_find
    (when-let [v (bfm/get bfm k)]
      (MapEntry. k v)))
  IPersistentCollection
  (empty [this] (CljBifurcanMap. (empty-bf-map)))
  (cons [this pair]
    #_conj
    (timbre/spy "cons")
    (timbre/spy pair)
    (CljBifurcanMap. (bfm/put bfm (first pair) (second pair))))
  (equiv [this other]
    (= bfm other))
  Seqable
  (seq [this]
    (timbre/spy "seq-ing")
    (sequence
      (map bfm-entry->clj-entry)
      (iterator-seq
        (.iterator ^IMap bfm))))
  ILookup
  (valAt [this k] (bfm/get bfm k))
  (valAt [this k not-found] (bfm/get bfm k not-found)))

(defn clj-bf-map
  "Wraps a Bifurcan map in a Clojure map-compatible type.
  The goal is to be able to use Bifurcan maps in Clojure code as if they were Clojure maps.
  Most operations in clojure.core should work, WIP"
  ([] (->CljBifurcanMap (empty-bf-map)))
  ([& keyvals]
   (transduce
     (comp
       (partition-all 2))
     conj
     (clj-bf-map)
     keyvals)))

(defmethod print-method Maps$Entry
  [entry ^Writer w]
  (timbre/info "Maps$Entry" entry)
  (let [class-name (type entry)]
    (if (= class-name Maps$Entry)
      (do
        (.write w "#io.lacuna.bifurcan.Maps$Entry")
        (.write w (str (bfm-entry->clj-entry entry)))
        ;(print-method (bfm-entry->clj-entry entry) w)
        #_(pr [(bfm-entry->clj-entry entry)]))
      (throw (ex-info "No print-method specified for this type"
               {:provided class-name
                :expected Maps$Entry})))))

(defmethod print-method Maps$HashEntry
  [entry ^Writer w]
  (let [class-name (type entry)]
    ;TODO if needed
    (throw (ex-info "No print-method specified for this type"
             {:provided class-name
              :expected Maps$HashEntry}))))

(defmethod print-dup Maps$Entry
  [entry w]
  (print-method entry w))

(comment
  ;Usage examples

  (count (clj-bf-map :a 1))

  (assoc (clj-bf-map) :a 1)

  (assoc (clj-bf-map) :a 1 :b 2)

  (find (assoc (clj-bf-map) :a 1) :a)

  (let [_
        (transduce
          (comp
            (map (fn [e] e))
            (partition-all 8))
          (completing
            (fn [accum pairs]
              (apply conj accum pairs)))
          (clj-bf-map)
          (repeatedly
            50
            (fn [] [(random-uuid) :x])))]
    )

  (conj (clj-bf-map) [:a 1] [:b 2])

  (clj-bf-map :a 1 :b 2 :c 3 :c 4)

  )
