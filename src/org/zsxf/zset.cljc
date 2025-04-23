(ns org.zsxf.zset
  ;clojurescript does not have +' and *' yet (arbitrary precision + and *)
  ;rename to match Clojure
  #?(:cljs (:refer-clojure :rename {+ +' * *'}))
  (:require [clojure.spec.alpha :as s]
            [org.zsxf.constant :as const]
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

(defn determine-weight
  "Potentially override zset-item weight"
  [_zset-item w]
  w)

(defn determine-weight-f
  "Potentially override zset-item weight"
  [_zset-item f]
  f)

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

(defn has-zset-weight? [x]
  (:zset/w (meta x)))

(defn zset-item
  ([x]
   (if (has-zset-weight? x)
     x
     (with-meta x const/zset-weight-of-1)))
  ([x weight]
   (if (meta x)
     ;meta exists, assoc to it
     (assoc-zset-item-weight x weight)
     ;optimization:
     ; reuse metadata map for common weights
     (if (= 1 weight)
       (with-meta x const/zset-weight-of-1)
       (assoc-zset-item-weight x weight)))))

(defn new-zset-item
  "Overwrites any existing meta"
  ([x]
   (with-meta x const/zset-weight-of-1))
  ([x weight]
   (if (= 1 weight)
     (with-meta x const/zset-weight-of-1)
     (with-meta x {:zset/w weight}))))

(defn zset-count-item
  "zset representing a count"
  [n]
  (zset-item const/zset-count n))

(defn zset-sum-item
  "zset representing a sum"
  [n]
  (zset-item const/zset-sum n))

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

(declare zset)

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
               (disj s prev-item)
               ;else keep item, adjust weight
               (conj (disj s prev-item) (assoc-zset-item-weight new-zsi new-weight))))
           ;else, new item
           (if (not= 0 (zset-weight new-zsi))
             (conj s new-zsi)
             s)))
       (fn [accum]
         accum))
     zset-1
     more)))

(defn zset
  ([coll]
   (zset (map identity) coll))
  ([xf coll]
   (zset+ (comp (map zset-item) xf) #{} coll)))

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
  ([indexed-zset-1 indexed-zset-2]
   (intersect-indexed* indexed-zset-1 indexed-zset-2 identity identity identity))
  ([indexed-zset-1 indexed-zset-2 zset*-item-1-f zset*-item-2-f pair-f]
   (let [commons (key-intersection indexed-zset-1 indexed-zset-2)]
     (into
       {}
       (map (fn [common]
              [common (zset*
                        (indexed-zset-1 common)
                        (indexed-zset-2 common)
                        zset*-item-1-f
                        zset*-item-2-f
                        pair-f)]))
       commons))))
