(ns org.zsxf.lib.result-set-xf.core
  (:require [clojure.spec.alpha :as s]
            [net.cgrand.xforms :as xforms]))

(defn- get-map-ns-keys
  "Return a vector of all keys from a map m that are namespaced under ns."
  [m ns]
  (into [] (filter (fn [k] (= ns (namespace k)))) (keys m)))

(defn- star-k
  [m k]
  (cond
    (= k '*) (vec (keys m))
    (and (qualified-symbol? k) (= "*" (name k))) (get-map-ns-keys m (namespace k))
    :else [k]))

(defn- star-filter
  [m select-keys-v]
  (transduce
    (comp
      (mapcat (fn [k] (star-k m k)))
      (distinct))
    conj
    []
    select-keys-v))

(defn pre-process [pattern]
  (transduce
    (xforms/by-key type identity
      (fn [_ [x :as xs]] (if (map? x) x xs))
      (xforms/into []))
    conj
    []
    pattern))

(defn- result-set-xf
  "Implementation of the pull algorithm."
  [result-set pattern]
  (let [[ks link-ops] (pre-process pattern)
        meta-xform (:xform (meta pattern) (map identity))]
    (transduce
      (comp
        meta-xform
        (xforms/by-key
          (fn [m] (select-keys m (star-filter m ks)))
          (fn [m] m)
          (fn [k ms] [k ms])
          (xforms/into []))
        (map (fn [[grouped-by-value grouped-maps-vector]]
               (merge grouped-by-value
                 (into {}
                   (map (fn [[item-name -pattern]]
                          ; return a vector of item-name and the result of calling trampoline
                          [item-name
                           ;for shallow recursion only, can blow up the stack!
                           (result-set-xf grouped-maps-vector -pattern)]))
                   link-ops)))))
      conj
      []
      result-set)))

(s/def ::pattern (and vector? (s/coll-of (s/or :keyword keyword? :map (s/map-of keyword? any?) :symbol symbol?))))
(s/def ::result-set-item (s/every-kv keyword? any?))
(s/def ::result-set (s/* ::result-set-item))

(defn pull
  "Reshapes tabular data into nested data.
  Takes a result-set and a pattern that describes the desired output.
  Returns a vector of items."
  [result-set pattern]
  (result-set-xf result-set pattern))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; Example usage:
(comment
  (pull
    #{{:track/name "Route 66" :artist/name "Alice" :db/id 42}
      {:track/name "Route 66" :artist/name "Bob" :db/id 43}}
    [:track/name {:track/artists [:db/id :artist/name]}]))
