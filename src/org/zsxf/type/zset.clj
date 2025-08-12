(ns org.zsxf.type.zset
  "ZSet internal data structure, wip
   {'x #_-> {:zset/w 1 :meta {}}}"
  (:require [org.zsxf.constant :as const]
            [taoensso.timbre :as timbre])
  (:import (clojure.lang Associative IEditableCollection IFn IHashEq IMapEntry IObj IPersistentMap IPersistentSet IPersistentVector SeqIterator)
           (java.util Set)))

;TODO do we need this protocol? or just use IMapEntry?
(defprotocol IZSetItem
  (item [_])
  (weight [_]))

(deftype ZSetItem [x weight]
  IZSetItem
  (item [_] x)
  (weight [_] weight)
  IMapEntry
  (key [_] x)
  (val [_] weight))

(declare zset)
(declare zset-item)

(defn zset-item [x weight]
  (->ZSetItem x weight))

(deftype ZSet [^IPersistentMap m]
  IPersistentSet
  (disjoin [this x]
    (throw
      ;TODO decide on best way to handle
      (ex-info "removal from a zset is expressed with data, disj (disjoin) not implemented" {})))
  ;cons working state
  (cons [this zsi]
    (timbre/info "cons" zsi m)
    (timbre/spy m)
    (let [w-provided (if (map-entry? zsi) (weight zsi) 1)   ;if no weight specified, set to 1
          k          (if (map-entry? zsi) (item zsi) zsi)
          zsi-data   (.valAt m k)
          next-w     (if zsi-data (+ (:zset/w zsi-data) w-provided) w-provided)]
      (timbre/spy next-w)
      (if-let [zm' (case (long next-w)
                     ;zero zset weight, remove
                     0 (.without m k)
                     ;all other cases, add
                     1 (.assoc ^Associative m k const/zset-weight-of-1)
                     nil)]
        (ZSet. zm')
        (ZSet. (.assoc ^Associative m k {:zset/w next-w})))))
  ;TODO continue here
  (seq [this]
    (timbre/spy ["seq" (count m)])
    (seq m))
  (empty [this]
    (ZSet. (-> {} (with-meta (meta m)))))
  (equiv [this other]
    (.equals this other))
  (get [this k]
    (when (.valAt m k) k))
  (count [this]
    (.count m))

  IObj
  (meta [this]
    (.meta ^IObj m))
  (withMeta [this m]
    (ZSet. (.withMeta ^IObj m m)))

  Object
  (toString [this]
    (timbre/info "toString")
    (str ":#{" (clojure.string/join " " (map str this)) "}"))
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
    ;(hasheq-ordered-set this)
    )

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
    ;(transient-ordered-set this)
    )
  IFn
  (invoke [this k] (when (.contains this k) k)))

(defn zset []
  (->ZSet {}))

(comment
  (->
    (zset)
    (conj (zset-item :a 2))
    (conj (zset-item :a -1))
    (conj (zset-item :a 1)))

  (->
    (zset)
    (conj :a)
    (conj :a)))
