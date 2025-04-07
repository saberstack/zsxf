(ns org.zsxf.zset
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.xf :as-alias xf]
            [taoensso.timbre :as timbre]))

;How to turn a zset into a proper Clojure collection
; Follow the example from https://github.com/clj-commons/ordered/blob/master/src/flatland/ordered/set.clj

(set! *print-meta* true)
(declare zset)

(timbre/set-ns-min-level! :trace)

(defn zset-weight
  "Get the weight of a zset item.
  x must have metadata with :zset/w integer value"
  [x]
  (:zset/w (meta x)))

(defn update-zset-item-weight
  [zset-item f]
  (vary-meta zset-item (fn [meta-map] (update meta-map :zset/w f))))

(defn assoc-zset-item-weight
  [zset-item w]
  (vary-meta zset-item (fn [meta-map] (assoc meta-map :zset/w w))))

(defn dissoc-weight [zset-item]
  (vary-meta zset-item (fn [meta-map] (dissoc meta-map :zset/w))))

(defn zset-sum+
  [f]
  (fn
    ([] 0)
    ([accum] accum)
    ([accum item]
     (let [w (zset-weight item)
           n (f item)]
       (+ accum (* w n))))))

(defn zset-count+
  ([] 0)
  ([accum] accum)
  ([accum item]
   (let [w (zset-weight item)]
     (+ accum w))))

;Optimization to save (a lot!) of memory.
;Reuse common zset weight maps
; TODO Can this be expanded further?
(defonce zset-weight-of-1 {:zset/w 1})

#_(defn zset-item
  ([x]
   (with-meta x zset-weight-of-1))
  ([x weight]
   ;reuse metadata map for common weights
   (if (= 1 weight)
     (with-meta x zset-weight-of-1)
     (with-meta x {:zset/w weight}))))

(defn zset-item
  ([x]
   (vary-meta x (fn [m] (merge (or m {}) zset-weight-of-1))))
  ([x weight]
   ;reuse metadata map for common weights
   (if (= 1 weight)
     (vary-meta x (fn [m] (merge (or m {}) zset-weight-of-1)))
     (vary-meta x (fn [m] (merge (or m {}) {:zset/w weight}))))))

;(defn zset-item?
;  [x]
;  (and
;    (util/can-meta? x)
;    (int? (zset-weight x))))

(defn zset-count-item
  "zset representing a count"
  [n]
  (zset-item [:zset/count] n))

(defn zset-sum-item
  "zset representing a sum"
  [n]
  (zset-item [:zset/sum] n))

(defn eligible-coll?
  "Check if a collection is eligible to be a zset item.
  All collections are eligible except map entries."
  [coll]
  (if (map-entry? coll)
    false
    (coll? coll)))

(s/def ::zset (s/and
                (s/coll-of (fn [x] (or (coll? x) (symbol? x))))
                (s/every (fn [set-item-map]
                           (some? (zset-weight set-item-map))))))

(defn zset?
  "Check if x conforms to the zset spec"
  [x]
  (let [result (s/valid? ::zset x)]
    (when-not result (timbre/error (s/explain-data ::zset x)))
    result))

(defn zset+
  "Adds two zsets"
  ([] (zset #{}))
  ([zset-1] zset-1)
  ([zset-1 zset-2]
   (zset+ zset-1 zset-2 (map identity)))
  ([zset-1 zset-2 xf]
   (transduce
     ;get set items one by one
     (comp cat xf)
     (completing
       (fn [s new-zset-item]
         (if-let [zset-item (s new-zset-item)]
           (let [new-weight (+ (zset-weight zset-item) (zset-weight new-zset-item))]
             (if (zero? new-weight)
               (disj s zset-item)
               (conj (disj s zset-item) (assoc-zset-item-weight new-zset-item new-weight))))
           (if (not= 0 (zset-weight new-zset-item))
             (conj s new-zset-item)
             s)))
       (fn [accum]
         accum))
     zset-1
     [zset-2])))

(defn zset-xf+
  "Takes a transducers and returns a function with the same signature as zset+.
  The transducer is applied to each new zset item from the second zset before adding it to the first zset."
  [xf]
  (fn
    ([] (zset #{}))
    ([zset-1] zset-1)
    ([zset-1 zset-2]
     (zset+ zset-1 zset-2 xf))))

(defn via-meta-zset-xf+
  "->xf-meta is a fn of one argument which accept the zset-2 metadata and returns a transducer.
  Returns a function with the same signature as zset+.
  The transducer is applied to each new zset item from the second zset before adding it to the first zset."
  [->xf-meta]
  (fn
    ([] (zset #{}))
    ([zset-1] zset-1)
    ([zset-1 zset-2]
     (zset+ zset-1 zset-2 (->xf-meta (meta zset-2))))))

(defn zset-pos+
  "Same as zset+ but does not maintain items with negative weight after +"
  ([]
   (zset #{}))
  ([zset-1]
   (zset-pos+ zset-1 (zset #{})))
  ([zset-1 zset-2]
   #_{:pre [(zset? zset-1) (zset? zset-2)]}
   (transduce
     ;get set items one by one
     (comp cat)
     (completing
       (fn [s new-zset-item]
         (if-let [zset-item (s new-zset-item)]
           (let [new-weight (+ (zset-weight zset-item) (zset-weight new-zset-item))]
             (if (or (zero? new-weight) (neg-int? new-weight))
               (disj s zset-item)
               (conj (disj s zset-item) (assoc-zset-item-weight new-zset-item new-weight))))
           (if (pos-int? (zset-weight new-zset-item))
             (conj s new-zset-item)
             s)))
       (fn [accum]
         accum))
     zset-1
     [zset-2])))

(defn zset-negate
  "Change the sign of all the weights in a zset"
  [zset]
  ;{:pre [(zset? zset)]}
  (transduce
    (map (fn [item]
           (update-zset-item-weight
             item
             (fn [prev-w] (* -1 prev-w)))))
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
   ;{:pre [(s/valid? (s/coll-of eligible-coll?) coll)]}
   (transduce
     xf
     (fn
       ([accum] accum)
       ([accum new-item]
        (timbre/spy accum)
        (timbre/spy new-item)
        (if-let [existing-item (accum new-item)]
          (let [accum'      (disj accum existing-item)
                existing-m' (update-zset-item-weight existing-item
                              (fn [prev-w]
                                ; fnil is used to handle the case where the weight is not present
                                ; in the meta, aka it is nil
                                ((fnil + 0) (zset-weight new-item) prev-w)))
                _           (timbre/spy existing-m')
                zset-w'     (zset-weight existing-m')]
            (if (zero? zset-w')
              accum'
              (conj accum' existing-m')))
          (conj accum (zset-item new-item weight)))))
     #{}
     coll)))

(defn zset-negative
  "Represents a deletion change"
  [coll]
  (zset coll (map identity) -1))

(defn zset*
  "Z-Sets multiplication implemented as per https://www.feldera.com/blog/SQL-on-Zsets#cartesian-products"
  [zset-1 zset-2]
  #_{:pre [(zset? zset-1) (zset? zset-2)]}
  (set
    (for [item-1 zset-1 item-2 zset-2]
      (let [weight-1   (zset-weight item-1)
            weight-2   (zset-weight item-2)
            new-weight (* weight-1 weight-2)]
        (zset-item
          ;zset weights of internal items don't serve a purpose after multiplication - remove them
          [(dissoc-weight item-1)                           ;remove weight
           (dissoc-weight item-2)]                          ;remove weight
          new-weight)))))

(defn index-xf
  "Returns a group-by-style transducer.
  Groups input items based on the return value of kfn.
  Each group is gathered into-coll (typically a set)."
  ([kfn]
   (index-xf kfn #{}))
  ([kfn into-coll]
   (xforms/by-key
     kfn
     (fn [zset-item] zset-item)
     (fn [k zset-of-grouped-items]
       (if k {k zset-of-grouped-items} {}))
     ;turn grouped items into a zset
     (xforms/into into-coll))))

(defn index
  "Convert a zset into a map indexed by a key function"
  #_(comment
      (index
        (zset #{{:name "Alice"} {:name "Alex"} {:name "Bob"}})
        (fn [m] (first (:name m)))))
  [zset kfn]
  (into {} (index-xf kfn) zset))

(defn indexed-zset->zset
  "Convert an indexed zset back into a zset"
  ([indexed-zset]
   (indexed-zset->zset indexed-zset (map identity)))
  ([indexed-zset xf]
   (into
     #{}
     (comp
       (mapcat (fn [[_k v]] v))
       xf)
     indexed-zset)))

(defn indexed-zset+
  "Adds two indexed zsets.
  Same as zset+ but for indexed zset which is a map."
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

(defn indexed-zset-pos+
  "Same as zset-pos+ but for indexed zset which is a map."
  ([]
   {})
  ([indexed-zset]
   (indexed-zset-pos+ indexed-zset {}))
  ([indexed-zset-1 indexed-zset-2]
   (transduce
     (map identity)
     (completing
       ;reduce function
       ; sum two indexed zsets, discard non-positive weight items
       (fn [indexed-zset-1-accum [k-2 zset-2]]
         (if (contains? indexed-zset-1-accum k-2)
           ;key exists in both indexed zsets, call zset-pos+ to add the zsets
           (let [new-zset (zset-pos+ (get indexed-zset-1-accum k-2) zset-2)]
             (if (= #{} new-zset)
               (dissoc indexed-zset-1-accum k-2)            ;remove key if zset is empty after zset addition
               (assoc indexed-zset-1-accum k-2 new-zset)))  ;else, add the new zset to the indexed zset map
           ;else...
           ;key does not exist, call zset-pos+ again to make sure we don't return negative weights
           (let [new-zset (zset-pos+ #{} zset-2)]
             (if (= #{} new-zset)
               ;return unchanged
               indexed-zset-1-accum
               ;else, add new zset-pos
               (assoc indexed-zset-1-accum k-2 new-zset))))))
     indexed-zset-1
     indexed-zset-2)))

(defn key-intersection
  "Taken from clojure.set/intersection but adapted to work for maps.
  Takes maps m1 and m2.
  Returns a set of common keys."
  [m1 m2]
  (if (< (count m2) (count m1))
    (recur m2 m1)
    (reduce
      (fn [result item]
        (if (contains? m2 item)
          (conj result item)
          result))
      #{}
      (keys m1))))

(defn join-indexed*
  "Join and multiply two indexed zsets (indexed zsets are maps)"
  [indexed-zset-1 indexed-zset-2]
  (let [commons (key-intersection indexed-zset-1 indexed-zset-2)]
    (into
      {}
      (map (fn [common] [common (zset* (indexed-zset-1 common) (indexed-zset-2 common))]))
      commons)))

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
