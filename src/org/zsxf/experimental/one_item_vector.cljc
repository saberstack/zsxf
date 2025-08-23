(ns org.zsxf.experimental.one-item-vector
  ;WIP, not for use
  "Memory-optimized vector implementation for single-element vectors.

  This namespace provides a specialized `OneItemVector` type that significantly
  reduces memory for vectors containing exactly one element.

  ## Usage

  Create optimized single-element vectors:
  ```clojure
  (vector-of-1 \"hello\")           ; Creates OneItemVector
  (optimize-vector [\"single\"])    ; Converts existing vector if beneficial
  ```

  ## Implementation Notes

  - OneItemVector implements all standard Clojure vector protocols
  - Seamlessly interoperates with regular vectors
  - Operations that would result in multi-element vectors automatically
    convert back to standard vectors

  ## Performance Characteristics

  - O(1) access operations (nth, peek, etc.)
  - Memory-efficient storage for single elements
  - Automatic conversion to regular vectors when size increases"

  ;;  ;Example memory usage on Clojure 1.12.1 via clj-memory-meter
  ;;
  ;;  (clj-memory-meter.core/measure
  ;;    (org.zsxf.type.one-item-vector/optimize-vector ["a"]))
  ;;  ;=> "64 B"
  ;;  (clj-memory-meter.core/measure
  ;;    ["a"])
  ;;  ;=> "424 B"
  (:refer-clojure :exclude [vector])
  #?(:clj
     (:import
      (clojure.lang Associative Counted IFn IHashEq ILookup IObj IPersistentCollection IPersistentStack
                    IPersistentVector Indexed MapEntry Reversible Seqable))))

#?(:clj
   (deftype OneItemVector [a meta]
     IObj
     (meta [this] meta)
     (withMeta [this m] (OneItemVector. a m))
     IPersistentCollection
     (empty [this] [])
     (equiv [this other] (.equiv [a] other))
     IHashEq
     (hasheq [this]
       (hash-ordered-coll [a]))
     Counted
     (count [this] 1)
     Seqable
     (seq [this] (seq [a]))
     IPersistentVector
     (length [this] 1)
     (cons [this x] (conj (with-meta [a] meta) x))
     (assocN [this idx v]
       (case idx
         0 (OneItemVector. v meta)
         (assoc (with-meta [a] meta) idx v)))
     Indexed
     (nth [this idx]
       (case idx
         0 a
         (nth [a] idx)))
     (nth [this idx nf]
       (case idx
         0 a
         nf))
     Associative
     (containsKey [this k]
       (case (int k)
         0 true
         false))
     (assoc [this idx v]
       (case (int idx)
         0 (OneItemVector. v meta)
         (assoc (with-meta [a] meta) idx v)))
     (entryAt [this idx]
       (case (int idx)
         0 (MapEntry. idx a)
         nil))
     IPersistentStack
     (peek [this] a)
     (pop [this] (with-meta [] meta))
     Reversible
     (rseq [this] (rseq [a]))
     ILookup
     (valAt [this idx]
       (nth this idx))
     (valAt [this idx nf]
       (nth this idx nf))
     IFn
     (invoke [this x]
       (.nth this x)))
   :cljs
   (comment
     ;TODO implement if needed
     ))

(defn vector-of-1 [a]
  #?(:clj  (->OneItemVector a nil)
     :cljs [a]))

(defn optimize-vector [v]
  #?(:clj
     (if (and
           (vector? v)
           (== 1 (count v))
           (not (instance? OneItemVector v)))
       ;optimize
       (->OneItemVector (v 0) (meta v))
       ;else, do not optimize
       v)
     ;TODO in CLJS (if relevant)
     :cljs v))
