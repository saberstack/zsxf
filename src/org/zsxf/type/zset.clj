(ns org.zsxf.type.zset
  "ZSet internal data structure
   {item #_-> weight}

   ZSet implementation details
   - zset items are ephemeral
   - used to convey weight information
   - data stored internally in a map wrapped in a deftype ZSet
   - deftype ZSet implements IPersistentSet
   - TODO disjoin is inherently incompatible with ZSets
      which convey ops via data with positive and negative weights"
  (:require [clj-memory-meter.core :as mm]
            [criterium.core :as crit]
            [flatland.ordered.map]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.constant :as const]
            [org.zsxf.util :as util]
            [org.zsxf.zset :as zs]
            [clojure.string :as str]
            [taoensso.timbre :as timbre])
  (:import (clojure.lang Associative IEditableCollection IFn IHashEq ILookup IMapEntry IObj
                         IPersistentCollection IPersistentMap IPersistentSet IPersistentVector ITransientAssociative ITransientMap ITransientSet
                         Indexed Keyword MapEntry MapEquivalence SeqIterator)
           (java.io Writer)
           (java.util Map Set)))

(declare zset)
(declare zsi)
(declare transient-zset)

(defmacro change! [field f & args]
  `(set! ~field (~f ~field ~@args)))

(defn- w->map [w]
  (if (== 1 w)
    const/zset-weight-of-1
    {:zset/w w}))

(defn meta-weight [x]
  (:zset/w (meta x)))

(defn- zsi-out-with-weight [x w]
  (timbre/info)
  (if (meta-weight x)
    x
    (vary-meta x (fnil conj {}) (w->map w))))

(defn zsi-out
  ([x w]
   (if (util/can-meta? x)
     (timbre/spy (zsi-out-with-weight x w))
     (do
       ;TODO decide how to return w in this case if needed
       (timbre/error "Emitting no weight for" x)
       (println)
       x))))

(deftype ZSItem [item ^long weight])

(defn zsi-weight [^ZSItem zsi]
  (.-weight zsi))

(defn zsi-item [^ZSItem zsi]
  (.-item zsi))

(defn calc-next-weight
  ^Object [w-now w-prev]
  (long
    (if (int? w-prev)
      ;w-prev already exists
      (+ w-prev w-now)
      ;new
      w-now)))

(defn any->zsi ^ZSItem [x]
  (if (instance? ZSItem x)
    x
    (zsi x 1)))

(defn any->weight ^long [x]
  (if-let [^ZSItem x (when (instance? ZSItem x) x)]
    (.-weight x)
    (or (:zset/w (meta x)) 1)))

(defn any->x ^Object [x]
  (if-let [^ZSItem x (when (instance? ZSItem x) x)]
    (.-item x)
    x))

(defn any->zsi-neg ^ZSItem [x]
  (if (instance? ZSItem x)
    (zsi (zsi-item x) (* -1 (zsi-weight x)))
    (zsi x -1)))

(defn- m-next ^IPersistentMap [^IPersistentMap m x w-next]
  (case (long w-next)
    ;zero zset weight, remove
    0 (.without m x)
    ;all other cases, add
    1 (.assoc ^Associative m x 1)
    (.assoc ^Associative m x w-next)))

(defn- m-next-pos [^IPersistentMap m x w-next]
  (if (neg-int? w-next)
    (.without m x)
    (m-next m x w-next)))

(deftype ZSet [^IPersistentMap m ^IPersistentMap meta-map ^boolean pos]
  IPersistentSet
  (disjoin [this x]
    ;WARN better to use data instead of disjoin
    ;implemented for compatibility with clojure.set/intersection and others
    (throw (ex-info "zsets do not support disjoin!" {}))
    (.cons this (any->zsi-neg x)))
  (cons [this x]
    (let [w-now  (any->weight x)
          x'     (any->x x)
          w-prev (.valAt m x')
          w-next (calc-next-weight w-now w-prev)]
      ;;(timbre/spy m)
      ;;(timbre/spy x')
      ;;(timbre/spy [w-prev w-next])
      ;;(println)
      (case pos
        false (ZSet. (m-next m x' w-next) meta-map pos)
        true (ZSet. (m-next-pos m x' w-next) meta-map pos))))

  (seq [this]
    ;(timbre/spy ["seq" (count m)])
    (sequence (map (fn [[x w]] (zsi-out x w))) m))
  (empty [this]
    (ZSet. {} meta-map pos))
  (equiv [this other]
    (.equals this other))
  (get [this k]
    ;behaves like get on a Clojure set
    (when (.valAt m k) k))
  (count [this]
    (.count m))

  IObj
  (meta [this] meta-map)
  (withMeta [this meta-map]
    (ZSet. m meta-map pos))

  Object
  (toString [this]
    (str "#zs #{" (str/join " " (map str this)) "}"))
  (hashCode [this]
    (reduce + (keep #(when (some? %) (.hashCode ^Object %)) (.seq this))))
  (equals [this other]
    (or (identical? this other)
      (and (instance? Set other)
        (let [^Set s other]
          (and (= (.size this) (.size s))
            (every? #(.contains s %) (.seq this)))))))

  IHashEq
  (hasheq [this]
    (hash-unordered-coll (or (keys m) {})))

  Set
  (iterator [this]
    (SeqIterator. (.seq this)))
  (contains [this k]
    (.containsKey m k))
  (containsAll [this ks]
    (every? #(.contains this %) ks))
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
    (.toArray this (object-array (.count this))))

  IEditableCollection
  (asTransient [this]
    (transient-zset this))
  IFn
  (invoke [this x] (when (.contains this x)
                     (let [[x w] (find m x)]
                       (zsi-out x w)))))

(defmacro m-next! [^ITransientMap m x w-next]
  ;need a macro to re-use this code that does mutation;
  ; it doesn't compile with mutable variables outside deftype
  `(case (long ~w-next)
     ;zero zset weight, remove
     0 (set! ~m (.without ~m ~x))
     ;all other cases, add
     1 (set! ~m (.assoc ^ITransientMap ~m ~x ^Object (long 1)))
     ;all other cases, add
     (set! ~m (.assoc ^ITransientMap ~m ~x ~w-next))))

(deftype TransientZSet [^{:unsynchronized-mutable true :tag ITransientMap} m ^boolean pos]
  ITransientSet
  (count [_]
    (.count m))
  (get [_ k]
    (when (.valAt m k) k))
  (disjoin [this x]
    ;WARN better to use data instead of disjoin
    ;implemented for compatibility with clojure.set/intersection and others
    (throw (ex-info "zsets do not support disjoin!" {}))
    (.conj this (any->zsi-neg x)))
  (conj [this x]
    (let [w-now  (any->weight x)
          x'     (any->x x)
          w-prev (.valAt m x')
          w-next (calc-next-weight w-now w-prev)]
      (when-not (= w-prev w-next)
        (case pos
          false (m-next! m x' w-next)
          true (if (neg-int? w-next)
                 (change! ^ITransientMap m .without x')
                 (m-next! m x' w-next))))
      this))
  (contains [_ k]
    (boolean (.valAt m k)))
  (persistent [_]
    (ZSet. (.persistent m) nil pos)))

(defn transient-zset [^ZSet a-zset]
  (TransientZSet. (transient (.-m a-zset)) (.-pos ^ZSet a-zset)))

(defn zset
  ([]
   (->ZSet {} nil false))
  ([& args]
   (into (zset) args)))

(defn zset-pos []
  (->ZSet {} nil true))

(defn zset-pos? [^ZSet s]
  (.-pos s))

(defn zsi
  (^ZSItem [x]
   (->ZSItem x 1))
  (^ZSItem [x weight]
   (->ZSItem x (long weight))))

(defn zsi? [x]
  (instance? ZSItem x))

(defn zset? [x]
  (instance? ZSet x))

(defn zset+
  ([] (zset))
  ([zset1] zset1)
  ([zset1 zset2]
   (into
     zset1
     (completing
       (fn [accum item+w]
         (conj accum item+w)))
     zset2))
  ([zset1 zset2 & more]
   (into
     zset1
     (comp
       cat
       (completing
         (fn [accum item+w]
           (conj accum item+w))))
     (concat
       [zset2]
       more))))

(defn zsi-from-reader [v]
  `(->ZSItem ~@v))

(defn zset-from-reader [s]
  `(into (zset) ~s))

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

(defmethod print-method ZSet [^ZSet zset, ^Writer w]
  (binding [*out* w]
    (let [m (.-m zset)]
      (print-meta zset w)
      (.write w "#zs ")
      (pr (into #{}
            (map (fn [^MapEntry v]
                   (zsi-out (nth v 0) (nth v 1))))
            m)))))

(defmethod print-method ZSItem [^ZSItem obj, ^Writer w]
  (binding [*out* w]
    (.write w "#zsi ")
    (pr [(.-item ^ZSItem obj) (.-weight ^ZSItem obj)])))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; End custom printing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn zset-negate [a-zset]
  (into
    (zset)
    (map (fn [a-zsi]
           (zsi (zsi-item a-zsi) (* -1 (zsi-weight a-zsi)))))
    a-zset))

(defn vector-split
  "Split vector into parts via subvec"
  [v n-parts]
  (if (zero? n-parts)
    []
    (let [len       (count v)
          part-size (quot len n-parts)
          remainder (rem len n-parts)]
      (mapv (fn [i]
              (let [start (+ (* i part-size) (min i remainder))
                    size  (+ part-size (if (< i remainder) 1 0))]
                (subvec v start (+ start size))))
        (range n-parts)))))

(defn vector-unsplit [v]
  (into [] (comp cat conj) v))

(defn vector-sum [v]
  (reduce + 0 v))

;Usage
(comment
  (->
    (zset)
    (conj (zsi :a 2))
    (conj (zsi :a -1))
    (conj (zsi :a 1)))
  (->
    (zset)
    (conj :a)
    (conj :a))

  (->
    (zset)
    (conj [:a])
    (conj [:a])))

;; Performance compare
(comment
  (time
    (def nums-v (into []
                  (comp
                    (map vector)
                    )
                  (range 10000000))))

  (time
    (def nums-v-split (vector-split nums-v 32)))

  (= nums-v (vector-unsplit nums-v-split))

  (time
    (vector-sum nums-v))

  (time
    (vector-sum
      (pmap vector-sum nums-v-split)))

  (crit/quick-bench
    (vector-sum nums-v))

  (crit/quick-bench
    (vector-sum
      (pmap vector-sum nums-v-split)))


  (time
    (reduce unchecked-add 0 (eduction (map first) nums-v)))

  (crit/quick-bench
    (do
      (zs/zset nums-v)
      :done))

  (time
    (do
      (zs/zset nums-v)
      :done))

  (mm/measure (zs/zset nums-v))

  (crit/quick-bench
    (do
      (into (zset) nums-v)
      :done))

  (time
    (do
      (into (zset) nums-v)
      :done))

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

  (mm/measure (into (zset-pos) nums-v))

  (zset+
    #zs #{#zsi [[:c] -1] #zsi [[:a] 42] #zsi [[:b] 4]}
    #zs #{#zsi [[:a] -42] #zsi [[:b] -4]}
    #zs #{#zsi [[:c] 2]})

  )

(comment
  ;wip list from prev zset impl
  ;public fns
  zs/zset-weight :ok
  ;convert value to zset-item
  zs/zset-item

  zs/zset+ :ok

  zs/zset-xf+
  zs/zset-pos+
  zs/zset-negate
  ;aggregates
  zset-sum+
  zset-count+
  ;indexed
  zs/indexed-zset+
  zs/indexed-zset-pos+

  ;public / aggregates
  zset-count-item
  zset-sum-item

  )


(set! *print-meta* true)
