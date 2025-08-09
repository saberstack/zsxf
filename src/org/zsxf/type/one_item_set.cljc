(ns org.zsxf.type.one-item-set
  "Optimizes a set with one item to a more efficient representation.
  This reduces memory usage when there are many sets with one item.
  Raw performance is a non-goal, focusing on memory usage instead."
  (:refer-clojure :exclude [hash-set set])
  #?(:clj
     (:import (clojure.lang IFn IHashEq IObj IPersistentCollection IPersistentSet Counted SeqIterator Seqable)
              (java.util Set))))


#?(:clj
   (deftype OneItemSet [item]

     IPersistentSet
     (get [this x] (when (= item x) item))
     (contains [this x] (= item x))
     (disjoin [this x] (if (= item x) #{} this))

     IObj
     ;Note: memory efficiency can potentially be improved by attaching meta to OneItemSet
     ; Currently we do not attach metadata to sets often so we defer to Clojure set meta impl
     (meta [_] nil)
     (withMeta [this meta']
       (with-meta #{item} meta'))

     Set
     (size [this] 1)
     (isEmpty [this] false)
     (iterator [this]
       (SeqIterator. (seq [item])))
     (containsAll [this s]
       (every? #(= % item) s))

     IPersistentCollection
     (empty [this] #{})
     (cons [this x]
       (conj #{item} x))
     (equiv [this other]
       (.equiv #{item} other))
     IHashEq
     (hasheq [this]
       (hash-unordered-coll #{item}))
     Counted
     (count [this] 1)
     Seqable
     (seq [this] (seq [item]))
     IFn
     (invoke [this x] (when (= item x) item))))

#?(:clj
   (defn hash-set
     "Creates a OneItemSet instance with a single item."
     [item]
     (->OneItemSet item)))

(defn set
  "Optimizes a set with one item to a OneItemSet instance."
  [s]
  #?(:clj
     (if (and
           (= 1 (count s))
           (not (instance? OneItemSet s))
           ;needed because sets with metadata not currently supported
           (nil? (meta s)))
       ;optimize
       (hash-set (first s))
       ;else, do not optimize
       s)
     ;TODO in CLJS (if relevant)
     :cljs s))
