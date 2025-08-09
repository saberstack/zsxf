(ns org.zsxf.zset
  "Zsets are like bags but also allow negative weights.

   Example:
   Imagine a table representing employee data.
   A Z-set could represent a change to this table.
   If you add a new employee, that employee's row would be in the Z-set with a weight of +1.
   If you delete an employee, their row would be in the Z-set with a weight of -1."
  ;clojurescript does not have +' and *' yet (arbitrary precision + and *)
  ;rename to match Clojure
  #?(:cljs (:refer-clojure :rename {+ +' * *'}))
  (:require [clojure.spec.alpha :as s]
            [org.zsxf.constant :as const]
            [org.zsxf.type.one-item-set :as ois]
            [org.zsxf.zset :as-alias zs]
            [org.zsxf.type.pair-vector :as pv]
            [org.zsxf.spec.zset]                            ;do not remove, loads clojure.spec defs
            [net.cgrand.xforms :as xforms]
            [taoensso.timbre :as timbre]))

;How to turn a zset into a proper Clojure collection (maybe in the future, if beneficial)
; Follow the example from https://github.com/clj-commons/ordered/blob/master/src/flatland/ordered/set.clj

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
    (vary-meta zset-item update :zset/w f')))

(defn assoc-zset-item-weight
  [zset-item w]
  (let [w' (determine-weight zset-item w)]
    (vary-meta zset-item assoc :zset/w w')))

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

(defn bool->weight
  "Convert a boolean to a zset weight."
  [bool]
  (case bool
    true 1
    false -1
    ;default, should not happen
    (throw (ex-info "Invalid boolean value" {:value bool}))))

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

(defn zset-count-item
  "zset singleton item representing a count"
  ([n]
   (zset-item const/zset-count n))
  ([n tag]
   (zset-item [tag const/zset-count] n)))

(defn zset-sum-item
  "zset singleton item representing a sum"
  ([n]
   (zset-item const/zset-sum n))
  ([n tag]
   (zset-item [tag const/zset-sum] n)))

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
  ([zset-1] zset-1)
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
       (fn [accum-final]
         (ois/set accum-final)))
     zset-1
     more)))

(defn zset
  ([coll]
   (zset (map identity) coll))
  ([xf coll]
   (zset+ (comp (map zset-item) xf) #{} coll)))

(defn zset-xf+
  "Takes a transducer and returns a function with the same signature as zset+.
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
     cat
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
         (ois/set accum)))
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
   (zset* zset-1 zset-2 identity identity))
  ([zset-1 zset-2 item-1-f item-2-f]
   #_{:pre [(zset? zset-1) (zset? zset-2)]}
   (set
     (for [item-1 zset-1 item-2 zset-2]
       (let [weight-1   (zset-weight item-1)
             weight-2   (zset-weight item-2)
             new-weight (*' weight-1 weight-2)]
         (zset-item
           (pv/vector
             (item-1-f (vary-meta item-1 dissoc-meta-weight)) ;remove weight
             (item-2-f (vary-meta item-2 dissoc-meta-weight))) ;remove weight
           new-weight))))))

(defn- index-xf-pair
  [k zset-of-grouped-items]
  (if k {k (ois/set zset-of-grouped-items)} {}))

(defn index-xf
  "Returns a group-by-style transducer.
  Groups input items based on the return value of kfn.
  Each group is gathered into-coll (typically a set)."
  [kfn]
  (xforms/by-key
    kfn
    identity                                                ;this is (fn [zset-item] zset-item)
    index-xf-pair
    ;turn grouped items into a zset
    (xforms/into #{})))

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
  [m1 m2]
  (let [[smaller larger] (if (< (count m1) (count m2)) [m1 m2] [m2 m1])]
    (transduce
      (map identity)
      (completing
        ;reduce function
        ; sum two indexed zsets, discard non-positive weight items
        (fn [indexed-zset-1-accum [k-2 zset-2]]
          (if (contains? indexed-zset-1-accum k-2)
            ;key exists in both indexed zsets, call zset-pos+ to add the zsets
            (let [new-zset (zset-pos+ (indexed-zset-1-accum k-2) zset-2)]
              (if (empty? new-zset)
                (dissoc indexed-zset-1-accum k-2)           ;remove key if zset is empty after zset addition
                (assoc indexed-zset-1-accum k-2 new-zset))) ;else, add the new zset to the indexed zset map
            ;else...
            ;key does not exist, call zset-pos+ again to make sure we don't return negative weights
            (let [new-zset (zset-pos+ #{} zset-2)]
              (if (empty? new-zset)
                ;return unchanged
                indexed-zset-1-accum
                ;else, add new zset-pos
                (assoc indexed-zset-1-accum k-2 new-zset))))))
      larger
      smaller)))

(defn key-intersection
  "Taken from clojure.set/intersection but adapted to work for maps.
  Takes maps m1 and m2.
  Returns a set of common keys."
  [m1 m2]
  (let [[smaller larger] (if (< (count m1) (count m2)) [m1 m2] [m2 m1])]
    (persistent!
      (reduce
        (fn [result item]
          (if (contains? larger item)
            (conj! result item)
            result))
        (transient #{})
        (keys smaller)))))

(defn intersect-indexed*
  "Intersect/join two indexed zsets (indexed zsets are maps)
  Returns an indexed zset.

  The weight of a common item in the return is the product (via zset*)
  of the weights of the same item in indexed-zset-1 and indexed-zset-2."
  ([indexed-zset-1 indexed-zset-2]
   (intersect-indexed* indexed-zset-1 indexed-zset-2 identity identity))
  ([indexed-zset-1 indexed-zset-2 zset*-item-1-f zset*-item-2-f]
   (let [commons (key-intersection indexed-zset-1 indexed-zset-2)]
     (into
       {}
       (map (fn [common]
              (pv/vector
                common
                (zset*
                  (indexed-zset-1 common)
                  (indexed-zset-2 common)
                  zset*-item-1-f
                  zset*-item-2-f))))
       commons))))
