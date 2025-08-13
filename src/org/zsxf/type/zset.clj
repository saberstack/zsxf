(ns org.zsxf.type.zset
  "ZSet internal data structure, wip
   {'x #_-> {:zset/w 1 :meta {}}}"
  (:require [org.zsxf.constant :as const]
            [taoensso.timbre :as timbre])
  (:import (clojure.lang Associative IEditableCollection IFn IHashEq IMapEntry IObj IPersistentMap IPersistentSet IPersistentVector Indexed MapEntry SeqIterator)
           (java.io Writer)
           (java.util Set)))

(declare zset)
(declare zset-item)
(declare zset-item?)

(defonce ^:dynamic *print-weight* false)

(defn set-print-weight! [x]
  (alter-var-root #'*print-weight* (constantly x)))

(defmacro change! [field f & args]
  `(set! ~field (~f ~field ~@args)))

(deftype ZSetItem [item weight]
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

(deftype ZSet [^IPersistentMap m ^IPersistentMap meta-m]
  IPersistentSet
  (disjoin [this x]
    (throw
      ;TODO decide on best way to handle
      (ex-info "removal from a zset is expressed with data, disj (disjoin) not implemented" {})))
  ;cons working state
  (cons [this x]
    (timbre/info "cons" x m)
    (timbre/spy m)
    (let [zsi?  (zset-item? x)
          k     (if zsi? (nth x 0) x)             ;if no weight specified, set to 1
          w     (if zsi? (nth x 1) 1)             ;wrap in a zset item if needed
          zsi-m (.valAt m k)
          w'    (if zsi-m (+ (:zset/w zsi-m) w) w)]
      (timbre/spy w')
      (if-let [m' (case (long w')
                    ;zero zset weight, remove
                    0 (.without m k)
                    ;all other cases, add
                    1 (.assoc ^Associative m k const/zset-weight-of-1)
                    nil)]
        (ZSet. m' meta-m)
        (ZSet. (.assoc ^Associative m k {:zset/w w'}) meta-m))))
  ;TODO continue here
  (seq [this]
    (timbre/spy ["seq" (count m)])
    (keys m))
  (empty [this]
    (ZSet. {} meta-m))
  (equiv [this other]
    (.equals this other))
  (get [this k]
    ;behaves like get on a Clojure set
    (when (.valAt m k) k))
  (count [this]
    (.count m))

  IObj
  (meta [this] meta-m)
  (withMeta [this meta-m]
    (ZSet. m meta-m))

  Object
  (toString [this]
    (timbre/info "toString")
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
    (hash-unordered-coll (keys m)))

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
  ;;TODO
  ;;IEditableCollection
  ;;(asTransient [this])
  IFn
  (invoke [this k] (when (.contains this k) k)))

(defn zset []
  (->ZSet {} nil))

(defn zsi [x weight]
  (->ZSetItem x weight))

(defn zsi-weight [zsi]
  (nth zsi 1))

(defn zset-item? [x]
  (instance? ZSetItem x))

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
              (map (fn [[k v]]

                     #zsi[k (:zset/w v)]))
              (map identity))
            (if *print-weight* m (keys m))) #_[(.-e d) (.-a d) (.-v d)])
      )))

(defmethod print-method ZSetItem [^ZSetItem itm, ^Writer w]
  (binding [*out* w]
    (.write w "#zsi")
    (pr [(nth itm 0) (nth itm 1)])))

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
  ; This does not seem possible until this is solved:
  ; https://clojure.atlassian.net/jira/software/c/projects/CLJ/issues/CLJ-2904
  (->ZSetItem item weight))

(defn zset-from-reader [s]
  ; This does not seem possible until this is solved:
  ; https://clojure.atlassian.net/jira/software/c/projects/CLJ/issues/CLJ-2904
  (into (zset) s))

;; Scratch

(comment

  (def nums-v (into [] (range 10000000)))

  (def m1 (into {}
            (map (fn [n] (MapEntry/create n const/zset-weight-of-1)))
            nums-v))

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
