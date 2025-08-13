(ns org.zsxf.type.zset
  "ZSet internal data structure, wip
   {item #_-> weight}"
  (:require [criterium.core :as crit]
            [org.zsxf.zset :as zs]
            [taoensso.timbre :as timbre])
  (:import (clojure.lang Associative IEditableCollection IFn IHashEq IMapEntry IObj IPersistentMap IPersistentSet IPersistentVector ITransientAssociative ITransientMap ITransientSet Indexed MapEntry SeqIterator)
           (java.io Writer)
           (java.util Set)))

(declare zset)
(declare zsi)
(declare zset-item)
(declare with-weight?)
(declare transient-zset)
(defonce ^:dynamic *print-weight* true)

(defn set-print-weight! [x]
  (alter-var-root #'*print-weight* (constantly x)))

(defmacro change! [field f & args]
  `(set! ~field (~f ~field ~@args)))

(deftype WithWeight [item weight]
  Indexed
  (nth [_ idx]
    (case idx
      0 item
      1 weight))
  (nth [_ idx nf]
    (case idx
      0 item
      1 weight
      nf)))

(defn- item [^WithWeight x]
  (.nth x 0))

(defn- weight [^WithWeight x]
  (.nth x 1))

(defn calc-next-weight
  [zsi w-prev]
  (if (int? w-prev)
    ;w-prev already exists
    (+ w-prev (weight zsi))
    ;new
    (weight zsi)))

(defn any->zsi [x]
  (if (instance? WithWeight x) x (zsi x 1)))

(defn disjoin-exception []
  (ex-info "removal from a zset is expressed with data, disj (disjoin) not implemented" {}))

(deftype ZSet [^IPersistentMap m ^IPersistentMap meta-map]
  IPersistentSet
  (disjoin [_ _x]
    (throw (disjoin-exception)))
  ;cons working state
  (cons [this x]
    ;(timbre/info "cons" x m)
    ;(timbre/spy m)
    (let [a-zsi  (any->zsi x)
          k      (item a-zsi)
          w-prev (.valAt m (item a-zsi))
          w-next (calc-next-weight a-zsi w-prev)
          m-next (case (long w-next)
                   ;zero zset weight, remove
                   0 (.without m k)
                   ;all other cases, add
                   1 (.assoc ^Associative m k 1)
                   (.assoc ^Associative m k w-next))]
      (ZSet. m-next meta-map)))
  (seq [this]
    ;(timbre/spy ["seq" (count m)])
    (sequence (map (fn [[x w]] (zsi x w))) m))
  (empty [this]
    (ZSet. {} meta-map))
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
    (ZSet. m meta-map))

  Object
  (toString [this]
    ;(timbre/info "toString")
    (str "#zset #{" (clojure.string/join " " (map str this)) "}")
    )
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
  (invoke [this k] (when (.contains this k) k)))

(deftype TransientZSet [^{:unsynchronized-mutable true :tag ITransientMap} m]
  ITransientSet
  (count [_]
    (.count m))
  (get [_ k]
    (when (.valAt m k) k))
  (disjoin [_ _x]
    (throw (disjoin-exception)))
  (conj [this x]
    (let [a-zsi  (any->zsi x)
          k      (item a-zsi)
          w-prev (.valAt m k)
          w-next (calc-next-weight a-zsi w-prev)]
      (when-not (= w-prev w-next)
        (case (long w-next)
          ;zero zset weight, remove
          0 (change! ^ITransientMap m .without k)
          ;all other cases, add
          1 (change! ^ITransientAssociative m .assoc k 1)
          ;all other cases, add
          (change! ^ITransientAssociative m .assoc k w-next)))
      this))
  (contains [_ k]
    (boolean (.valAt m k)))
  (persistent [_]
    (ZSet. (.persistent m) nil)))

(defn transient-zset [^ZSet a-zset]
  (TransientZSet. (transient (.-m a-zset))))

;;(comment
;;  (let [t (transient #zset #{#zsi [:a 42] #zsi [:b 2]})]
;;    (persistent!
;;      (conj! t (zsi :c 3)))))

(defn zset []
  (->ZSet {} nil))

(defn zsi [x weight]
  (->WithWeight x weight))

(defn with-weight? [x]
  (instance? WithWeight x))

(defn zset+2
  [zset1 zset2]
  (transduce
    (map identity)
    (completing
      (fn [accum item+w]
        (conj accum item+w)))
    zset1
    zset2))

;;(comment
;;  (zset+2
;;    #zset #{#zsi [:a 42] #zsi [:b 2]}
;;    #zset #{#zsi [:a 42] #zsi [:b -2]}))

(comment
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
           (ois/optimize-set accum-final)))
       zset-1
       more))))

(comment
  (->
    (zset)
    (conj (zsi :a 2))
    (conj (zsi :a -1))
    (conj (zsi :a 1)))
  (->
    (zset)
    (conj :a)
    (conj :a)))

(defn zsi-from-reader [[item weight]]
  (->WithWeight item weight))

(defn zset-from-reader [s]
  (into (zset) s))

;; Scratch
(comment
  (def nums-v (into [] (range 10000000)))

  (def s1 (into #{} (shuffle nums-v)))
  (def s2 (into #{} (shuffle nums-v)))

  (time (= s1 s2))

  (def s3 (into #{} nums-v))
  (def s4 (into #{} nums-v))

  (time (= s3 s4))

  (def s5 (conj s4 -1))

  (time (= s4 s5))

  (def s-from-m1
    (into #{}
      (map (fn [kv] (kv 0)))
      m1)))

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
      (.write w "#zset ")
      (pr (into #{}
            (if *print-weight*
              (map (fn [v]
                     (zsi (nth v 0) (nth v 1))))
              (map identity))
            (if *print-weight* m (keys m))) #_[(.-e d) (.-a d) (.-v d)])
      )))

(defmethod print-method WithWeight [^WithWeight obj, ^Writer w]
  (binding [*out* w]
    (.write w "#zsi")
    (pr [(nth obj 0) (nth obj 1)])))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; End custom printing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;; Performance compare
(comment

  (time
    (def nums-v (into [] (map vector) (range 10000000))))

  (crit/quick-bench
    (do
      (zs/zset nums-v)
      :done))

  (crit/quick-bench
    (do
      (into (zset) nums-v)
      :done))

  )
