(ns org.zsxf.zset
  ;clojurescript does not have +' and *' yet (arbitrary precision + and *)
  ;rename to match Clojure
  #?(:cljs (:refer-clojure :rename {+ +' * *'}))
  (:require [clojure.spec.alpha :as s]
            [org.zsxf.relation :as rel]
            [org.zsxf.zset :as-alias zs]
            [org.zsxf.spec.zset]                            ;do not remove, loads clojure.spec defs
            [net.cgrand.xforms :as xforms]
            [taoensso.timbre :as timbre]))

;How to turn a zset into a proper Clojure collection
; Follow the example from https://github.com/clj-commons/ordered/blob/master/src/flatland/ordered/set.clj

(set! *print-meta* true)

(defn zset-weight
  "Get the weight of a zset item.
  x must have metadata with :zset/w integer value"
  [x]
  (:zset/w (meta x)))

(defn determine-weight [zset-item w]
  ;TODO determine if it's important to keep [:not-found] weights at 1
  #_(if (rel/type-not-found? zset-item)
      (min 1 w)
      w)
  w)

(defn determine-weight-f [zset-item f]
  ;TODO determine if it's important to keep [:not-found] weights at 1
  #_(if (rel/type-not-found? zset-item)
      (comp #(min 1 %) f)
      f)
  f)

(defonce zset-weight-of-1-f (fn [_] 1))

(defn update-zset-item-weight
  [zset-item f]
  (let [f' (determine-weight-f zset-item f)]
    (vary-meta zset-item (fn [meta-map] (update meta-map :zset/w f')))))

(defn assoc-zset-item-weight
  [zset-item w]
  (let [w' (determine-weight zset-item w)]
    (vary-meta zset-item (fn [meta-map] (assoc meta-map :zset/w w')))))

(defn dissoc-meta-weight [meta-map]
  (dissoc meta-map :zset/w))

(defn zset-sum+
  [f]
  (fn
    ([] 0)
    ([accum] accum)
    ([accum item]
     (let [w (zset-weight item)
           n (f item)]
       (+' accum (*' w n))))))

(defn zset-count+
  ([] 0)
  ([accum] accum)
  ([accum item]
   (let [w (zset-weight item)]
     (+' accum w))))

;Optimization to save (a lot!) of memory.
;Reuse common zset weight maps
(defonce zset-weight-of-1 {:zset/w 1})

(defn zset-item
  ([x]
   (with-meta x zset-weight-of-1))
  ([x weight]
   (if (meta x)
     ;meta exists, assoc to it
     (assoc-zset-item-weight x weight)
     ;optimization:
     ; reuse metadata map for common weights
     (if (= 1 weight)
       (with-meta x zset-weight-of-1)
       (assoc-zset-item-weight x weight)))))

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

(defn zset?
  "Check if x conforms to the zset spec"
  [x]
  (let [result (s/valid? ::zs/zset x)]
    (when-not result (timbre/error (s/explain-data ::zs/zset x)))
    result))

(defn zset
  ;TODO remove or reuse zset+ impl
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
        (if-let [existing-item (accum new-item)]
          ;existing item
          (let [accum'      (disj accum existing-item)
                existing-m' (update-zset-item-weight existing-item
                              (fn [prev-w]
                                ; fnil is used to handle the case where the weight is not present
                                ; in the meta, aka it is nil
                                ((fnil + 0) (zset-weight new-item) prev-w)))
                _           existing-m'
                zset-w'     (zset-weight existing-m')]
            (if (zero? zset-w')
              accum'
              (conj accum' existing-m')))
          ; most new items do not have an existing weight, but if they do, we use it
          (if-let [existing-weight (zset-weight new-item)]
            ;skip if existing weight is zero
            (if (zero? existing-weight)
              accum
              ;else, add item with existing weight
              (conj accum (zset-item new-item existing-weight)))
            ;new item, no weight, add the item with default weight
            (conj accum (zset-item new-item weight))))))
     #{}
     coll)))

(defn with-not-found [s zsi-not-found]
  (vary-meta s
    (fn [m]
      (update m :zset/deny-not-found
        (fn [s] (conj (or s #{}) zsi-not-found))))))

(defn without-not-found [s zsi-not-found]
  (vary-meta s
    (fn [m]
      (update m :zset/deny-not-found
        (fn [s] (disj (or s #{}) zsi-not-found))))))

(defn zset-denied-not-found [s]
  (:zset/deny-not-found (meta s)))

(defn deny-not-found? [s zsi]
  (contains? (zset-denied-not-found s) zsi))

(defn zset+
  "Adds two zsets"
  ([] (zset #{}))
  ([zset-1] (zset+ #{} zset-1))
  ([zset-1 zset-2]
   (zset+ (map identity) zset-1 zset-2))
  ([xf zset-1 & more]
   ;{:pre [(zset? zset-1) (zset? zset-2)]}
   (transduce
     ;get set items one by one
     (comp cat xf)
     (completing
       (fn [s new-zsi]
         (if-let [prev-item (s new-zsi)]
           ;item already exists in the zset
           (let [new-weight (+' (zset-weight prev-item) (zset-weight new-zsi))]
             (if (zero? new-weight)
               ;remove item
               (if-let [zsi-not-found (rel/maybe-zsi->not-found prev-item)]
                 ;removing a Maybe item, adjust metadata
                 (without-not-found (disj s prev-item) zsi-not-found)
                 ;regular item, just remove
                 (disj s prev-item))
               ;else keep item, adjust weight
               (conj (disj s prev-item) (assoc-zset-item-weight new-zsi new-weight))))
           ;else, new item
           (if (not= 0 (zset-weight new-zsi))
             (let [zsi-not-found (rel/maybe-zsi->not-found new-zsi)
                   deny-nf?      (deny-not-found? s new-zsi)]
               (if deny-nf?
                 s
                 (if zsi-not-found
                   (with-not-found
                     (conj (disj s zsi-not-found) new-zsi)
                     zsi-not-found)
                   (conj s new-zsi))))
             s)))
       (fn [accum]
         accum))
     zset-1
     more)))

(defn zset-xf+
  "Takes a transducers and returns a function with the same signature as zset+.
  The transducer is applied to each new zset item from the second zset before adding it to the first zset."
  [xf]
  (fn
    ([] (zset #{}))
    ([zset-1] zset-1)
    ([zset-1 zset-2]
     (zset+ xf zset-1 zset-2))))

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
           (let [new-weight (+' (zset-weight zset-item) (zset-weight new-zset-item))]
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
             (fn [prev-w] (*' -1 prev-w)))))
    conj
    #{}
    zset))

(defn zset-negative
  "Represents a deletion change"
  [coll]
  (zset coll (map identity) -1))

(defn zset*
  "Z-Sets multiplication implemented as per https://www.feldera.com/blog/SQL-on-Zsets#cartesian-products"
  ([zset-1 zset-2]
   (zset* zset-1 zset-2 identity identity identity))
  ([zset-1 zset-2 item-1-f item-2-f pair-f]
   #_{:pre [(zset? zset-1) (zset? zset-2)]}
   (set
     (for [item-1 zset-1 item-2 zset-2]
       (let [weight-1   (zset-weight item-1)
             weight-2   (zset-weight item-2)
             new-weight (*' weight-1 weight-2)]
         (pair-f
           (zset-item
             [(item-1-f (vary-meta item-1 dissoc-meta-weight)) ;remove weight
              (item-2-f (vary-meta item-2 dissoc-meta-weight))] ;remove weight
             new-weight)))))))

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
  ;determine size and switch map order (if needed) for improved efficiency
  (if (< (count m1) (count m2))
    (recur m2 m1)
    (reduce
      (fn [result item]
        (if (contains? m1 item)
          (conj result item)
          result))
      #{}
      (keys m2))))

(defn intersect-indexed*
  "Intersect/join two indexed zsets (indexed zsets are maps)
  Returns an indexed zset.

  The weight of a common item in the return is the product (via zset*)
  of the weights of the same item in indexed-zset-1 and indexed-zset-2."
  [indexed-zset-1 indexed-zset-2]
  (let [commons (key-intersection indexed-zset-1 indexed-zset-2)]
    (into
      {}
      (map (fn [common]
             [common (zset*
                       (indexed-zset-1 common)
                       (indexed-zset-2 common))]))
      commons)))

(defn left-join-indexed*
  "Like intersect-indexed* but keeps everything from indexed-zset-1
   with missing matches from indexed-zset-2 replaced with :not-found"
  ([indexed-zset-1 indexed-zset-2]
   (left-join-indexed* indexed-zset-1 indexed-zset-2 rel/not-found))
  ([indexed-zset-1 indexed-zset-2 nf]
   (let [left-join-indexed*-return
         (transduce
           (map (fn [k+v] k+v))
           (completing
             (fn [accum [index-k-1 zset-1 :as k+v]]
               (if-let [[_index-k-2 zset-2] (find indexed-zset-2 index-k-1)]
                 (assoc accum index-k-1 (zset* zset-1 zset-2 identity identity rel/mark-as-opt-rel))
                 (assoc accum index-k-1 (zset* zset-1 zset-1 identity (fn [_] nf) rel/mark-as-opt-rel)))))
           {}
           indexed-zset-1)]
     left-join-indexed*-return)))
