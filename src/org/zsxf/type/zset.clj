(ns org.zsxf.type.zset
  "ZSet internal data structure as a map:
   {item #_-> weight}

   ZSet implementation details
   - zset items are ephemeral
   - used to convey weight information
   - data stored internally in a map wrapped in a deftype ZSet
   - deftype ZSet implements IPersistentSet
   - disjoin is inherently incompatible with zsets
      which convey ops via data with positive and negative weights"
  (:require [clj-memory-meter.core :as mm]
            [criterium.core :as crit]
            [flatland.ordered.map]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.constant :as const]
            [org.zsxf.util :as util]
            [org.saberstack.clojure.inline :as inline]
            [org.zsxf.zset :as zs]
            [clojure.string :as str]
            [clojure.set :as set]
            [taoensso.timbre :as timbre])
  (:import (clojure.lang
             IFn IObj IHashEq Associative IPersistentCollection MapEntry IPersistentSet
             ITransientSet ITransientMap IEditableCollection SeqIterator Seqable)
           (java.io Writer)
           (java.util Set)))

(declare zset)
(declare zsi)
(declare transient-zset)

(defmacro change! [field f & args]
  `(set! ~field (~f ~field ~@args)))

(defn- w->map [w]
  (if (== 1 w)
    const/zset-weight-of-1
    {:zset/w w}))

(defmacro zset-weight [x]
  `(:zset/w (meta ~x)))

(defn -zset-weight [x]
  (zset-weight x))

(defmacro calc-next-weight-inline [w-now w-prev]
  `(long
     (if (int? ~w-prev)
       ;w-prev already exists
       (+ ~w-prev ~w-now)
       ;new
       ~w-now)))

(defmacro zset-item
  ([x] `(zset-item ~x 1))
  ([x w]
   `(if-let [meta-map# (meta ~x)]
      ;meta exists, assoc to it
      (inline/meta-vary-meta meta-map# ~x assoc :zset/w ~w)
      ;optimization:
      ; reuse metadata map for common weights
      (if (== 1 ~w)
        (with-meta ~x ~const/zset-weight-of-1)
        (with-meta ~x {:zset/w ~w})))))

(defn- zsi-out-with-weight [x w]
  (inline/vary-meta-x x (fnil conj {}) (w->map w)))

(defn zsi-out
  ([x w]
   (cond
     (zset-weight x) x
     (util/can-meta? x) (zsi-out-with-weight x w)
     :else
     (do
       ;TODO decide how to return w in this case if needed
       (timbre/error "Emitting no weight for" x)
       (println)
       x))))

(defn any->zsi-neg [x]
  (let [w (if-let [w (zset-weight x)] (* -1 w) -1)]
    (zset-item x w)))

(defmacro m-next-inline ^clojure.lang.IPersistentMap [^clojure.lang.IPersistentMap m x w-next ?w-prev]
  `(if (and (nil? ~?w-prev) (= 1 ~w-next))
     (.assoc ~m ~x ^Long (long 1))
     (case (long ~w-next)
       ;zero zset weight, remove
       0 (.without ~m ~x)
       ;all other cases, add
       1 (.assoc ^Associative (.without ~m ~x) ~x 1)
       (.assoc ^Associative (.without ~m ~x) ~x ~w-next))))

(defmacro m-next! [^ITransientMap m ^Object x w-next ?w-prev]
  ;need a macro to re-use this code that does mutation;
  ; it doesn't compile with mutable variables outside deftype
  `(if (and (nil? ~?w-prev) (= 1 ~w-next))
     (set! ~m (.assoc ^ITransientMap ~m ~x ^Long (long 1)))
     (case (long ~w-next)
       ;zero zset weight, remove
       0 (set! ~m (.without ~m ~x))
       ;all other cases, add
       1 (set! ~m (.assoc ^ITransientMap (.without ~m ~x) ~x ^Long (long 1)))
       ;all other cases, add
       (set! ~m (.assoc ^ITransientMap (.without ~m ~x) ~x ^Long (long ~w-next))))))

(defn set-debug [s value]
  (set! *print-meta* true)
  {:s-value (s value)
   :first-s (first s)})


(defn -zset-item
  ([x] (zset-item x))
  ([x w] (zset-item x w)))

(defmacro zset-weight-inline [x]
  `(:zset/w (meta ~x)))

(defmacro neg-int?-inline
  "Return true if x is a negative fixed precision integer"
  [x] `(and (int? ~x) (neg? ~x)))

(deftype ZSet [^clojure.lang.IPersistentMap m ^clojure.lang.IPersistentMap meta-map ^boolean pos]
  Seqable
  (seq [_] (keys m))

  IPersistentCollection
  (cons [_ x]
    (let [?w-now  (zset-weight-inline x)
          w-now   (or ?w-now 1)
          ?w-prev (.valAt m x)
          w-next  (calc-next-weight-inline w-now ?w-prev)
          x'      (zset-item x w-next)]
      (case pos
        false (ZSet. (m-next-inline m x' w-next ?w-prev) meta-map pos)
        true (ZSet. (if (neg-int?-inline w-next)
                      (.without m x')
                      (m-next-inline m x' w-next ?w-prev))
               meta-map pos))))
  (empty [_]
    (ZSet. {} meta-map pos))
  (equiv [this other]
    (.equals this other))
  (count [_]
    (.count m))

  IPersistentSet
  (disjoin [this x]
    (.cons this (any->zsi-neg x)))
  (get [_ x]
    (let [[x' w] (find m x)]
      (zsi-out x' w)))

  IFn
  (invoke [_ x]
    (let [[x' w] (find m x)]
      (zsi-out x' w)))

  IObj
  (meta [_] meta-map)
  (withMeta [_ new-meta-map]
    (ZSet. m new-meta-map pos))

  IHashEq
  (hasheq [this]
    (hash-unordered-coll this))

  IEditableCollection
  (asTransient [this]
    (transient-zset this))

  Object
  (toString [this]
    (str "#zs #{" (str/join " " (map str this)) "}"))
  (hashCode [this]
    (reduce + (keep #(when (some? %) (.hashCode ^Object %)) (.seq this))))
  (equals [this other]
    (or (identical? this other)
      (and (instance? Set other)
        (let [^Set other' other]
          (and (= (.size this) (.size other'))
            (every? #(.contains other' %) (keys m)))))))

  Set
  (iterator [this]
    (SeqIterator. (.seq this)))
  (contains [_this x]
    (.containsKey m x))
  (containsAll [this xs]
    (every? #(.contains this %) xs))
  (size [this]
    (.count this))
  (isEmpty [this]
    (zero? (.count this)))
  (^objects toArray [this ^objects dest]
    (reduce (fn [idx item]
              (aset dest idx item)
              (inc idx))
      0
      (.seq this))
    dest)
  (toArray [this]
    (.toArray this (object-array (.count this)))))

(def ^{:static true} zset-empty (->ZSet {} nil false))

(deftype TransientZSet [^{:unsynchronized-mutable true :tag ITransientMap} m ^boolean pos]
  ITransientSet
  (count [_]
    (.count m))
  (get [_this x]
    (let [[x w] (find m x)]
      (zsi-out x w)))
  (disjoin [this x]
    (.conj this (any->zsi-neg x)))
  (conj [this x]
    (let [?w-now  (zset-weight-inline x)
          w-now   (or ?w-now 1)
          ?w-prev (.valAt m x)
          w-next  (calc-next-weight-inline w-now ?w-prev)
          x'      (zset-item x w-next)]
      (case pos
        false (m-next! m x' w-next ?w-prev)
        true (if (neg-int?-inline w-next)
               (change! ^ITransientMap m .without x')
               (m-next! m x' w-next ?w-prev)))
      this))
  (contains [_ x]
    (boolean (.valAt m x)))
  (persistent [_]
    (ZSet. (.persistent m) nil pos)))

(defn transient-zset [^ZSet a-zset]
  (TransientZSet. (transient (.-m a-zset)) (.-pos ^ZSet a-zset)))

(defn- create-empty-zset []
  (->ZSet {} nil false))
(def create-empty-zset-memo (memoize create-empty-zset))

(defn- create-empty-zset-pos []
  (->ZSet {} nil true))
(def create-empty-zset-pos-memo (memoize create-empty-zset-pos))

(defmacro zset
  ([] `~zset-empty)
  ([coll] `(into zset-empty ~coll)))

(defmacro hash-zset
  "Creates zset from items (via macro for performance)."
  [item]
  `(conj (zset) ~item))

(comment

  (let [xf (comp (map vector) (map zset-item))
        s1 (into (zset) xf (range 0 10000))
        s2 (into (zset) xf (range 5000 15000))
        s3 (into (zset) xf (range 10000 20000))]
    (crit/quick-bench
      (set/union s1 s2 s3)))

  (get (->
         (zset)
         (transient)
         (conj! (with-meta ["42"] {:zset/w 1 :meta "a"}))
         (conj! (with-meta ["42"] {:zset/w 1 :meta "b"}))
         (persistent!))
    ["42"])

  (get (->
         (zset)
         (conj (with-meta ["42"] {:zset/w 1 :meta "a"}))
         (conj (with-meta ["42"] {:zset/w 1 :meta "b"})))
    ["42"]))

(defn zset-pos
  ([]
   (create-empty-zset-pos-memo))
  ([coll]
   (into (zset-pos) coll)))

(defn zset-pos? [^ZSet z]
  (.-pos z))

(defmacro zsi [x]
  `(zset-item ~x))

(defn zset? [x]
  (instance? ZSet x))

(defn zsi-from-reader [v]
  `(zset-item ~@v))

(defn zset-from-reader [s]
  `(zset ~s))

(defn zset-pos-from-reader [s]
  `(zset-pos ~s))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

(defmethod print-method ZSet [^ZSet z, ^Writer w]
  (binding [*out* w]
    (let [m (.-m z)]
      (print-meta z w)
      (.write w "#zs")
      (when (zset-pos? z)
        (.write w "p"))
      (.write w " ")
      (pr (into #{}
            (map (fn [^MapEntry v]
                   (zsi-out (nth v 0) (nth v 1))))
            m)))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; End custom printing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- cast-zset [z]
  (if (zset? z) z (zset z)))

(defn zset+
  ([] (zset))
  ([z1] (zset z1))
  ([z1 z2 & more]
   (apply set/union (cast-zset z1) (cast-zset z2) (map cast-zset more))))

(defn- cast-zset-pos [z]
  (if (and (zset? z) (zset-pos? z)) z (zset-pos z)))

(defn zset-pos+
  ([] (zset-pos))
  ([z1] z1)
  ([z1 z2 & more]
   (cast-zset-pos
     (apply zset+ z1 z2 more))))

(defn zset-xf+
  "Takes a transducer and returns a function with the same signature as zset+.
  The transducer is applied to each new zset item from the second zset before adding it to the first zset."
  [xf]
  (fn
    ([] (zset))
    ([z1] z1)
    ([z1 z2]
     (into z1 xf z2))))

(defn zset-negate [z]
  (into
    (zset)
    (map (fn [x]
           (any->zsi-neg x)))
    z))

(defn indexed-zset+
  "Adds two indexed zsets.
  Same as zset+ but for indexed zset which is a map."
  ([]
   {})
  ([iz] iz)
  ([iz1 iz2]
   (merge-with zset+ iz1 iz2))
  ([iz1 iz2 & more]
   (apply merge-with zset+ iz1 iz2 more)))

(defn indexed-zset-pos+
  "Adds two indexed zsets.
  Same as zset-pos+ but for indexed zset which is a map."
  ([] {})
  ([iz] iz)
  ([iz1 iz2]
   (merge-with zset-pos+ iz1 iz2))
  ([iz1 iz2 & more]
   (apply merge-with zset-pos+ iz1 iz2 more)))


(defn- index-xf-pair
  [k zset-of-grouped-items]
  (if k {k zset-of-grouped-items} {}))

(defn- index-xf
  "Returns a group-by-style transducer.
  Groups input items based on the return value of kfn.
  Each group is gathered into-coll (typically a set)."
  [kfn empty-z]
  (xforms/by-key
    kfn
    identity                                                ;this is (fn [zset-item] zset-item)
    index-xf-pair
    ;turn grouped items into a zset
    (xforms/into empty-z)))

(defn index
  "Convert a zset into a map indexed by a key function.
  Works for zset and zset-pos."
  [z kfn]
  (into {} (index-xf kfn (empty z)) z))

(defn indexed-zset->zset
  "Convert an indexed zset back into a zset"
  ([indexed-zset]
   (indexed-zset->zset indexed-zset (map identity)))
  ([indexed-zset xf]
   (into
     (zset)
     (comp
       (mapcat (fn [k+v] (nth k+v 1)))
       xf)
     indexed-zset)))

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

;Usage
(comment
  (let [zs (zset #{{:name "Alice"} {:name "Alex"} {:name "Bob"}})
        iz (index zs (fn [m] (first (:name m))))]
    (indexed-zset+ iz iz))

  (let [zsp (zset-pos #{{:name "Alice"} {:name "Alex"} {:name "Bob"}})
        iz  (index zsp (fn [m] (first (:name m))))]
    (indexed-zset-pos+ iz iz)))

(defn zset*
  "Z-Sets multiplication"
  ([z1 z2]
   (zset* z1 z2 identity identity))
  ([z1 z2 item1-f item2-f]
   (zset
     (for [item1 z1 item2 z2]
       (let [w1    (zset-weight item1)
             w2    (zset-weight item2)
             w-new (* w1 w2)]
         (zset-item
           (vector
             (item1-f (inline/vary-meta-x item1 dissoc :zset/w))
             (item2-f (inline/vary-meta-x item2 dissoc :zset/w)))
           w-new))))))

(defn intersect-indexed*
  "Intersect/join two indexed zsets (indexed zsets are maps)
  Returns an indexed zset.

  The weight of a common item in the return is the product (via zset*)
  of the weights of the same item in indexed-zset-1 and indexed-zset-2."
  ([iz1 iz2]
   (intersect-indexed* iz1 iz2 identity identity))
  ([iz1 iz2 zset*-item1-f zset*-item2-f]
   (let [commons (util/key-intersection iz1 iz2)]
     (into
       {}
       (map (fn [common]
              (vector
                common
                (zset*
                  (iz1 common)
                  (iz2 common)
                  zset*-item1-f
                  zset*-item2-f))))
       commons))))

;Usage
(comment
  (let [zs (zset #{{:name "Alice"} {:name "Alex"} {:name "Bob"}})
        iz (index zs (fn [m] (first (:name m))))]
    (intersect-indexed*
      (indexed-zset+ iz iz)
      (indexed-zset+ iz iz))))

;Usage
(comment
  (zset*
    (zset #{(zsi {:a 41} 2) (zsi {:a 42} 2)})
    (zset #{(zsi {:a 41} 2) (zsi {:a 42} 2)})))

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

(comment
  (=
    #zs #{(zset-count-item 42 :counter1)}
    #zs #{(zset-count-item 42 :counter1)})
  (=
    (zset+
      #zs #{(zset-count-item 42 :counter1)}
      #zs #{(zset-count-item 42 :counter1)})
    #{^#:zset {:w 84} [:counter1 [:zset/count]]}))

;; Performance compare
(comment
  (zset+
    #zs #{#zsi [[:c] -1] #zsi [[:a] 42] #zsi [[:b] 4]}
    #zs #{#zsi [[:a] -42] #zsi [[:b] -4]}
    #zs #{#zsi [[:c] 2]})

  (do
    (time (def nums-v (into []
                        (comp
                          (map vector)
                          (map zset-item)
                          )
                        (range 1000000))))

    (System/gc)

    (time (do (def z1 (zs/zset nums-v)) :done))
    (Thread/sleep 1000)
    (time (do (doall (seq z1)) :done))

    (Thread/sleep 1000)

    (time (do (def z2 (zset nums-v)) :done))
    (Thread/sleep 1000)
    (time (do (doall (seq z2)) :done)))

  (crit/quick-bench
    (do (zs/zset nums-v) :done))

  (crit/quick-bench
    (do (zset nums-v) :done))

  (mm/measure (zs/zset nums-v))

  (mm/measure (zset nums-v))

  (crit/quick-bench
    (do
      (doall (seq z1))
      :done))

  (crit/quick-bench
    (do
      (doall (seq z2))
      :done))

  (time
    (do
      (def s1 (set nums-v))
      :done))

  (time
    (run!
      (fn [x] x)
      s1))

  (time
    (do
      (def s1' (transduce (map identity) conj #{} nums-v))
      :done))

  (time
    (run!
      (fn [x] x)
      s1'))

  (time
    (run!
      (fn [x] x)
      z1))

  (time
    (def nums-v-split (util/vector-split nums-v 32)))

  (= nums-v (util/vector-unsplit nums-v-split))

  (time
    (util/vector-sum nums-v))

  (time
    (util/vector-sum
      (pmap util/vector-sum nums-v-split)))

  (crit/quick-bench
    (util/vector-sum nums-v))

  (crit/quick-bench
    (util/vector-sum
      (pmap util/vector-sum nums-v-split)))

  (do
    (time (do (def z2 (zset nums-v)) :done))
    (time (run! (fn [x] x) z2)))

  (first
    (sequence
      (comp
        (map (fn [x w] (zsi [x] w)))
        (xforms/reduce
          (completing
            (fn into-zset
              ([] (zset-pos))
              ([accum item+w]
               (conj accum item+w))))))
      [:a :b :c :d]
      (cycle [-1 1])))

  (mm/measure (into (zset-pos) nums-v)))



(comment
  ;public fns
  zs/zset-weight :OK zset-weight
  ;convert value to zset-item
  zs/zset-item :OK zset-item

  zs/zset+ :OK zset+

  zs/zset-xf+ :OK zset-xf+
  zs/zset-pos+ :OK zset-pos+
  zs/zset-negate :OK zset-negate
  zs/zset* :OK zset*
  zs/index :OK index
  zs/intersect-indexed* :OK intersect-indexed*

  ;aggregates
  zs/zset-sum+ :OK zset-sum+
  zs/zset-count+ :OK zset-count+
  ;indexed
  zs/indexed-zset+ :OK indexed-zset+
  zs/indexed-zset-pos+ :OK indexed-zset-pos+

  ;public / aggregates
  zs/zset-count-item :OK zset-count-item
  zs/zset-sum-item :OK zset-sum-item
  )

(set! *print-meta* true)
