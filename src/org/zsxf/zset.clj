(ns org.zsxf.zset
  (:require [org.zsxf.zset :as-alias zset]
            [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [net.cgrand.xforms :as xforms]
            [taoensso.timbre :as timbre]))

;How to turn a zset into a proper Clojure collection
; Follow the example from https://github.com/clj-commons/ordered/blob/master/src/flatland/ordered/set.clj

(set! *print-meta* true)
(declare zset)

(timbre/set-ns-min-level! :error)

(defn zset-weight
  "Get the weight of a zset item, typically a map"
  [m]
  (:zset/w (meta m)))

(defn eligible-coll?
  "Check if a collection is eligible to be a zset item.
  All collections are eligible except map entries."
  [coll]
  (if (map-entry? coll)
    false
    (coll? coll)))

(s/def ::zset (s/and
                (s/coll-of coll?)
                (s/every (fn [set-item-map]
                           (some? (zset-weight set-item-map))))))

(defn zset? [x]
  (let [result (s/valid? ::zset x)]
    (when-not result (timbre/error (s/explain-data ::zset x)))
    result))

(defn- common-keep-and-remove
  [zset-1 zset-2 commons]
  (transduce
    (comp
      (map (fn [common]
             (timbre/spy common)
             (let [meta-1 (meta (zset-1 common))
                   meta-2 (meta (zset-2 common))]
               (with-meta common (merge-with + meta-1 meta-2)))))
      ;group items in two buckets: "remove" and "keep" based on weight
      (xforms/by-key
        (fn [common] (if (zero? (zset-weight common))
                       :common-remove
                       :common-keep))
        (fn [common] common)
        (fn [remove-or-keep commons] [remove-or-keep commons])
        (xforms/into #{})))
    (completing
      (fn [accum item]
        (conj accum item))
      (fn [accum]
        accum))
    {}
    commons))

(defn zset+
  "Z-Sets addition implemented as per https://www.feldera.com/blog/Z-sets/#z-sets"
  ([]
   (zset #{}))
  ([zset-1]
   (zset+ zset-1 (zset #{})))
  ([zset-1 zset-2]
   ;{:pre [(zset? zset-1) (zset? zset-2)]}
   (let [commons (clojure.set/intersection zset-1 zset-2)
         {:keys [common-keep common-remove]} (common-keep-and-remove zset-1 zset-2 commons)]
     (transduce
       ;get set items one by one
       (comp cat)
       (completing
         (fn [accum set-item]
           (if (zero? (zset-weight set-item))
             ;remove common-remove items
             (disj accum set-item)
             ;keep everything else
             (conj accum set-item)))
         (fn [accum]
           accum))
       #{}
       [common-keep
        zset-1
        zset-2
        ;common-remove items **must** stay at the end here to be properly removed during the reduction!
        common-remove]))))

(defn zset-negate
  "Change the sign of all the weights in a zset"
  [zset]
  {:pre [(zset? zset)]}
  (transduce
    (map (fn [item]
           (vary-meta item
             (fn [prev-meta]
               (update prev-meta :zset/w (fn [prev-w] (* -1 prev-w)))))))
    conj
    #{}
    zset))

(defn zset
  "Collection to zset as per https://www.feldera.com/blog/Implementing%20Z-sets/#converting-a-collection-to-a-z-set"
  ([coll]
   (zset coll (map identity)))
  ([coll xf]
   (zset coll xf 1))
  ([coll xf weight]
   {:pre [(s/valid? (s/coll-of eligible-coll?) coll)]}
   (transduce
     xf
     (fn
       ([accum]
        (timbre/spy accum))
       ([accum m]
        (timbre/spy accum)
        (timbre/spy m)
        (if-not (coll? m)
          m
          (if-let [existing-m (accum m)]
            (let [accum'      (disj accum existing-m)
                  existing-m' (vary-meta existing-m
                                (fn [prev-meta]
                                  (update prev-meta :zset/w
                                    (fn [prev-w]
                                      ; fnil is used to handle the case where the weight is not present
                                      ; in the meta, aka it is nil
                                      ((fnil + 0) (zset-weight m) prev-w)))))
                  _           (timbre/spy existing-m')
                  zset-w'     (zset-weight existing-m')]
              (if (zero? zset-w')
                accum'
                (conj accum' existing-m')))
            (conj accum (with-meta m {:zset/w weight}))))))
     #{}
     coll)))

(defn zset-negative
  "Represents a deletion change"
  [coll]
  (zset coll (map identity) -1))

(defn zset*
  "Z-Sets multiplication implemented as per https://www.feldera.com/blog/SQL-on-Zsets#cartesian-products"
  [zset-1 zset-2]
  {:pre [(zset? zset-1) (zset? zset-2)]}
  (set (for [m-1 zset-1 m-2 zset-2]
         (with-meta
           [m-1 m-2]
           ;{m-1 m-2}
           {:zset/w (* (zset-weight m-1) (zset-weight m-2))}))))

(defn index
  "Convert a zset into a map indexed by a key function"
  #_(comment
      (index
        (zset #{{:name "Alice"} {:name "Alex"} {:name "Bob"}})
        (fn [m] (first (:name m)))))
  [zset kfn]
  (into {}
    (xforms/by-key
      kfn
      (fn [m] m)
      (fn [k ms]
        (if k {k ms} {}))
      (xforms/into #{}))
    zset))

(defn indexed-zset->zset
  "Convert an indexed zset back into a zset"
  [indexed-zset]
  (transduce
    (mapcat (fn [[_k v]] v))
    conj
    #{}
    indexed-zset))

(defn indexed-zset+
  ([]
   {})
  ([indexed-zset]
   (indexed-zset+ indexed-zset {}))
  ([indexed-zset-1 indexed-zset-2]
   ;TODO merge-with is likely slow and needs optimization
   ; https://github.com/bsless/clj-fast
   (merge-with zset+ indexed-zset-1 indexed-zset-2))
  ([indexed-zset-1 indexed-zset-2 & args]
   (apply merge-with zset+ indexed-zset-1 indexed-zset-2 args)))

(defn join
  "Join two indexed zsets as a map relation. Does not multiply zsets."
  [indexed-zset-1 indexed-zset-2]
  (let [commons (clojure.set/intersection (set (keys indexed-zset-1)) (set (keys indexed-zset-2)))]
    (transduce
      (map (fn [common] [(indexed-zset-1 common) (indexed-zset-2 common)]))
      conj
      {}
      commons)))

(defn join-indexed*
  "Join and multiply two indexed zsets"
  [indexed-zset-1 indexed-zset-2]
  (let [commons (clojure.set/intersection (set (keys indexed-zset-1)) (set (keys indexed-zset-2)))]
    (transduce
      (map (fn [common] [common (zset* (indexed-zset-1 common) (indexed-zset-2 common))]))
      conj
      {}
      commons)))

(defn indexed-zset*
  ;TODO fix or remove
  [indexed-zset-1 indexed-zset-2]
  (merge-with zset* indexed-zset-1 indexed-zset-2))

(comment

  (zset [{:a 1} {:b 2}])                                    ;ok
  (zset [{:a 1} 1])                                         ;error


  (zset+
    #{^#:zset {:w 2} {:name "Alice"}
      ^#:zset {:w -5} {:name "Bob"}
      ^#:zset {:w -10} {:name "Clara"}}

    #{^#:zset {:w -4} {:name "Alice"}
      ^#:zset {:w 5} {:name "Bob"}
      ^#:zset {:w 1} {:name "Clara"}})

  (zset+
    #{^#:zset {:w 2} {:name "Alice"}}

    #{^#:zset {:w -1} {:name "Bob"}})

  (zset+
    #{^#:zset {:w 1} {:name "Alice"}}

    #{^#:zset {:w -1} {:name "Alice"}})

  (zset+
    #{^#:zset {:w 2} {:name "Alice"}}

    #{^#:zset {:w 0} {:name "Bob"}})


  (zset-negate #{^#:zset {:w 1} {:name "Alice"}})

  (zset-negate #{^#:zset {:w -1} {:name "Bob"}})

  (zset*
    #{^#:zset {:w 3} {:name "Alice"}
      ^#:zset {:w 5} {:name "Bob"}}
    #{^#:zset {:w 3} {:name "Alice"}
      ^#:zset {:w 5} {:name "Bob"}})

  (zset*
    (zset [{:name "T" :team/id 1}])
    (zset [{:name "P" :person/team 1} {:name "P_2" :person/team 1}]))
  )

;Next
; https://www.feldera.com/blog/Implementing%20Z-sets/
; https://www.feldera.com/blog/SQL-on-Zsets/
; Use core.async to achieve incremental computation

;Misc
; Calcite functionality that (potentially) supports incremental view maintenance
; https://youtu.be/iT4k5DCnvPU?t=890
; https://calcite.apache.org/javadocAggregate/org/apache/calcite/rel/stream/Delta.html (differentiation)

;; Incremental computation via core.async

(defonce *computation-state (atom {}))

(defn register-computation! [xf atom]

  (a/chan xf))

(defn insert->zset [& maps]
  (zset `#{~@maps}))

(defn delete->zset [& maps]
  (zset `#{~@maps}))
