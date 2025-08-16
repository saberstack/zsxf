(ns org.zsxf.type.zset
  "ZSet internal data structure, wip
   {item #_-> weight}"
  (:require [clj-memory-meter.core :as mm]
            [criterium.core :as crit]
            [flatland.ordered.map]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.zset :as zs]
            [taoensso.timbre :as timbre])
  (:import (clojure.lang Associative IEditableCollection IFn IHashEq IMapEntry IObj
                         IPersistentCollection IPersistentMap IPersistentSet ITransientAssociative ITransientMap ITransientSet
                         Indexed Keyword MapEntry SeqIterator)
           (java.io Writer)
           (java.util Set)))

;TODO Continue here

(declare zset)
(declare zset-cons)
(declare zsi)
(declare with-weight?)
(declare transient-zset)
(defonce ^:dynamic *print-weight* true)

(defn set-print-weight! [x]
  (alter-var-root #'*print-weight* (constantly x)))

(defmacro change! [field f & args]
  `(set! ~field (~f ~field ~@args)))

(deftype ZSItem [item weight]
  IPersistentCollection
  (equiv [this other]
    (= item other))

  IHashEq
  (hasheq [this]
    (hash item))

  Object
  (equals [this other]
    (or
      (identical? this other)
      (= item other)))
  (hashCode [this]
    (.hashCode item)))

(defn calc-next-weight
  [zsi w-prev]
  (if (int? w-prev)
    ;w-prev already exists
    (+ w-prev (.-weight ^ZSItem zsi))
    ;new
    (.-weight ^ZSItem zsi)))

(defn any->zsi ^ZSItem [x]
  (if (instance? ZSItem x) x (zsi x 1)))

(defn disjoin-exception []
  (ex-info "removal from a zset is expressed with data, disj (disjoin) not implemented" {}))

(defn- m-next ^IPersistentMap [^IPersistentMap m k w-next]
  (case (long w-next)
    ;zero zset weight, remove
    0 (.without m k)
    ;all other cases, add
    1 (.assoc ^Associative m k 1)
    (.assoc ^Associative m k w-next)))

(defn- m-next-pos [^IPersistentMap m k w-next]
  (if (neg-int? w-next)
    (.without m k)
    (m-next m k w-next)))

(deftype ZSet [^IPersistentMap m ^IPersistentMap meta-map ^boolean pos]
  IPersistentSet
  (disjoin [_ _x]
    (throw (disjoin-exception)))
  (cons [this x]
    (let [a-zsi  (any->zsi x)
          k      (.-item ^ZSItem a-zsi)
          w-prev (.valAt m (.-item ^ZSItem a-zsi))
          w-next (calc-next-weight a-zsi w-prev)]
      (case pos
        true (ZSet. (m-next-pos m k w-next) meta-map pos)
        false (ZSet. (m-next m k w-next) meta-map pos))))
  (seq [this]
    (println "seq")
    (sequence (map (fn [[x w]] (zsi x w))) m))
  (empty [this]
    (ZSet. {} meta-map pos))
  (equiv [this other]
    (println "equiv")
    (.equals this other))
  (get [this k]
    (println "get")
    ;behaves like get on a Clojure set
    (when (.valAt m k) k))
  (count [this]
    (println "count")
    (.count m))

  IObj
  (meta [this] meta-map)
  (withMeta [this meta-map]
    (ZSet. m meta-map pos))

  Object
  (toString [this]
    (timbre/info "toString")
    (str "#zs #{" (clojure.string/join " " (map str this)) "}"))
  (hashCode [this]
    (println "hashCode")
    (reduce + (keep #(when (some? %) (.hashCode ^Object %)) (.seq this))))
  (equals [this other]
    (println "equals")
    (or (identical? this other)
      (and (instance? Set other)
        (let [^Set s other]
          (and (= (.size this) (.size s))
            (every? #(.contains s %) (.seq this)))))))

  IHashEq
  (hasheq [this]
    (println "hasheq")
    (hash-unordered-coll (or (keys m) {})))

  Set
  (iterator [this]
    (println "iterator")
    (SeqIterator. (.seq this)))
  (contains [this k]
    (println "contains")
    (.containsKey m k))
  (containsAll [this ks]
    (println "containsAll")
    (every? #(.contains this %) ks))
  (size [this]
    (println "size")
    (.count this))
  (isEmpty [this]
    (println "isEmpty")
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
  (invoke [this k] (when (.contains this k)
                     (let [[k weight] (find m k)]
                       (zsi k weight)))))

(defmacro m-next! [^ITransientMap m ^Object k w-next]
  ;need a macro to re-use this code that does mutation;
  ; it doesn't compile with mutable variables outside deftype
  `(case (long ~w-next)
     ;zero zset weight, remove
     0 (set! ~m (.without ~m ~k))
     ;all other cases, add
     1 (set! ~m (.assoc ^ITransientMap ~m ~k ^Object (long 1)))
     ;all other cases, add
     (set! ~m (.assoc ^ITransientMap ~m ~k ~w-next))))

(deftype TransientZSet [^{:unsynchronized-mutable true :tag ITransientMap} m ^boolean pos]
  ITransientSet
  (count [_]
    (.count m))
  (get [_ k]
    (when (.valAt m k) k))
  (disjoin [_ _x]
    (throw (disjoin-exception)))
  (conj [this x]
    (let [a-zsi  (any->zsi x)
          k      (.-item ^ZSItem a-zsi)
          w-prev (.valAt m k)
          w-next (calc-next-weight a-zsi w-prev)]
      (when-not (= w-prev w-next)
        (case pos
          false (m-next! m k w-next)
          true (if (neg-int? w-next)
                 (change! ^ITransientMap m .without k)
                 (m-next! m k w-next))))
      this))
  (contains [_ k]
    (boolean (.valAt m k)))
  (persistent [_]
    (ZSet. (.persistent m) nil pos)))

(defn transient-zset [^ZSet a-zset]
  (TransientZSet. (transient (.-m a-zset)) (.-pos ^ZSet a-zset)))

;;(comment
;;  (let [t (transient #zs #{#zsi [:a 42] #zsi [:b 2]})]
;;    (persistent!
;;      (conj! t (zsi :c 3)))))

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

(defn zset+2
  [zset1 zset2]
  (into
    zset1
    (completing
      (fn [accum item+w]
        (conj accum item+w)))
    zset2))

(defn zset-pos+2 [zset1 zset]
  )



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
  (->ZSItem item weight))

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
      (.write w "#zs ")
      (pr (into #{}
            (if *print-weight*
              (map (fn [^MapEntry v]
                     (zsi (nth v 0) (nth v 1))))
              (map identity))
            (if *print-weight* m (keys m))) #_[(.-e d) (.-a d) (.-v d)])
      )))

(defmethod print-method ZSItem [^ZSItem obj, ^Writer w]
  (binding [*out* w]
    (.write w "#zsi")
    (pr [(.-item ^ZSItem obj) (.-weight ^ZSItem obj)])))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; End custom printing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;In progress:
; - efficient zset-pos+
;

;; Performance compare
(comment

  (time
    (def nums-v (into [] (map vector) (range 10000000))))

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
        (map (fn [x w] (zsi x w)))
        (xforms/reduce
          (completing
            (fn into-zset
              ([] (zset-pos))
              ([accum item+w]
               (conj accum item+w))))))
      [:a :b :c :d]
      (cycle [-1 1])))

  (mm/measure (into (zset-pos) nums-v))

  )

(comment
  (zset+2
    #zs #{#zsi [:a 42] #zsi [:b 4]}
    #zs #{#zsi [:a -42] #zsi [:b -4]})

  (zset-po))
