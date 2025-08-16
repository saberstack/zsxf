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
                         IPersistentCollection IPersistentMap IPersistentSet ITransientAssociative ITransientSet ITransientSet
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
  [w-x w-prev]
  (if (int? w-prev)
    ;w-prev already exists
    (+ w-prev w-x)
    ;new
    w-x))

(defn x->zsi ^ZSItem [^Object x w-next]
  ;if zsi, return, else make it
  (if (instance? ZSItem x) x (zsi x w-next)))

(defn zsi->x ^Object [zsi]
  (if (instance? ZSItem zsi)
    (.-item ^ZSItem zsi)
    zsi))

(defn x->weight ^long [x]
  (if (instance? ZSItem x) (.-weight ^ZSItem x) 1))

(defn disjoin-exception []
  (ex-info "removal from a zset is expressed with data, disj (disjoin) not implemented" {}))

(defn- m-next ^IPersistentSet [^IPersistentSet s x w-next]
  (case (long w-next)
    ;zero zset weight, remove
    0 (.disjoin s x)
    (-> s
      (.disjoin x)
      (.cons ^IPersistentSet (zsi (zsi->x x) w-next)))))

(defn- m-next-pos [^IPersistentSet s x w-next]
  (if (neg-int? w-next)
    (.disjoin s x)
    (m-next s x w-next)))

(deftype ZSet [^IPersistentSet s ^IPersistentSet meta-map ^boolean pos]
  IPersistentSet
  (disjoin [_ _x]
    (throw (disjoin-exception)))
  (cons [this x]
    (let [w-x    (x->weight x)
          w-prev (when-let [zsi-prev (s x)] (.-weight ^ZSItem zsi-prev))
          w-next (calc-next-weight w-x w-prev)]
      (if (= w-prev w-next)
        this
        (case pos
          true (ZSet. (m-next-pos s x w-next) meta-map pos)
          false (ZSet. (m-next s x w-next) meta-map pos)))))
  ;(cons2 [this x])
  (seq [this]
    (seq s))
  (empty [this]
    (ZSet. #{} meta-map pos))
  (equiv [this other]
    (.equals this other))
  (get [this x]
    ;behaves like get on a Clojure set
    (when (s x) x))
  (count [this]
    (.count s))

  IObj
  (meta [this] meta-map)
  (withMeta [this meta-map]
    (ZSet. s meta-map pos))

  Object
  (toString [this]
    (str "#zs #{" (clojure.string/join " " (map str this)) "}"))
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
    (hash-unordered-coll s))

  Set
  (iterator [this]
    (SeqIterator. (.seq this)))
  (contains [this x]
    (.contains s x))
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
    (.toArray this (object-array (.count this))))

  IEditableCollection
  (asTransient [this] (transient-zset this))
  IFn
  (invoke [this x]
    (s x)))
(comment
  (defn- m-next ^IPersistentSet [^IPersistentSet s x w-next]
    (case (long w-next)
      ;zero zset weight, remove
      0 (.disjoin s x)
      (-> s
        (.disjoin x)
        (.cons ^IPersistentSet (zsi (zsi->x x) w-next))))))

(defmacro m-next! [^ITransientSet s# ^Object x# w-next#]
  ;need a macro to re-use this code that does mutation;
  ; it doesn't compile with mutable variables outside deftype
  `(case (long ~w-next#)
     ;zero zset weight, remove
     0 (set! ~s# (.disjoin ~s# ~x#))
     ;all other cases, add
     (let [s'# (-> ~s#
               (.disjoin ^ITransientSet ~x#)
               (.conj ^ITransientSet (zsi (zsi->x ~x#) ~w-next#)))]
       (set! ~s# s'#))))

(deftype TransientZSet [^{:unsynchronized-mutable true :tag ITransientSet} s ^boolean pos]
  ITransientSet
  (count [_]
    (.count s))
  (get [_ x]
    (when (.get s x) x))
  (disjoin [_ _x]
    (throw (disjoin-exception)))
  (conj [this x]
    (let [w-x    (x->weight x)
          w-prev (when-let [zsi-prev (.get s x)] (.-weight ^ZSItem zsi-prev))
          w-next (calc-next-weight w-x w-prev)]
      (when-not (= w-prev w-next)
        (case pos
          false (m-next! s x w-next)
          true (if (neg-int? w-next)
                 (change! ^ITransientSet s .disjoin x)
                 (m-next! s x w-next))))
      this))
  (contains [_ k]
    (boolean (.get s k)))
  (persistent [_]
    (ZSet. (.persistent s) nil pos))
  IFn
  (invoke [this x]
    (s x)))

(defn transient-zset [^ZSet a-zset]
  (TransientZSet. (transient (.-s a-zset)) (.-pos ^ZSet a-zset)))

(defn zset
  (^ZSet []
   (->ZSet #{} nil false))
  (^ZSet [& args]
   (into (zset) args)))

(comment
  (meta ((transient #{(with-meta 'a {:meta 42}) 'b 'c}) 'a))
  ;=> {:meta 42}

  )

(defn zset-pos
  (^ZSet []
   (->ZSet #{} nil true))
  (^ZSet [& args]
   (into (->ZSet #{} nil true) args)))

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

;;(defmethod print-dup ZSet [^ZSet zset, ^Writer w]
;;  (print-ctor zset (fn [^ZSet o w] (print-dup (.-s o) w)) w))

(defmethod print-method ZSet [^ZSet zset, ^Writer w]
  (binding [*out* w]
    (let [s (.s zset)]
      (print-meta zset w)
      (.write w "#zs ")
      (pr s)
      )))

;;(defmethod print-dup ZSItem [^ZSItem zsi, ^Writer w]
;;  (print-ctor zsi
;;    (fn [^ZSItem o ^Writer w]
;;      (.write w "#zsi")
;;      (print-dup [(.-item o) (.-weight ^ZSItem o)] w)) w))

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


  (time
    (def set-of-nums (into #{} nums-v)))

  (time
    (def set-of-nums-2 (into #{} nums-v)))

  (time
    (= set-of-nums set-of-nums-2))

  (time
    (def zset-of-nums (into (zset) nums-v)))

  (time
    (def old-zset-of-nums (zs/zset nums-v)))

  (time
    (= zset-of-nums set-of-nums))

  (time
    (= old-zset-of-nums set-of-nums))

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
    #zs #{#zsi [:a -42] #zsi [:b -5]}))
