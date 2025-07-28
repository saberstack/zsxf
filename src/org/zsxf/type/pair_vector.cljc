(ns org.zsxf.type.pair-vector
  #?(:clj
     (:import
      (clojure.lang Associative Counted IHashEq ILookup IObj IPersistentCollection IPersistentStack
                    IPersistentVector Indexed MapEntry Reversible Seqable))))

(declare pair-vector)

#?(:clj
   (deftype PairVector [a b meta]
     IObj
     (meta [this] meta)
     (withMeta [this m] (PairVector. a b m))
     IPersistentCollection
     (empty [this] [])
     (equiv [this other] (.equiv [a b] other))
     IHashEq
     (hasheq [this]
       (hash-ordered-coll [a b]))
     Counted
     (count [this] 2)
     Seqable
     (seq [this] (seq [a b]))
     IPersistentVector
     (length [this] 2)
     (cons [this x] (conj (with-meta [a b] meta) x))
     (assocN [this idx v]
       (case idx
         0 (PairVector. v b meta)
         1 (PairVector. a v meta)
         (assoc (with-meta [a b] meta) idx v)))
     Indexed
     (nth [this idx]
       (case idx
         0 a
         1 b
         (nth [a b] idx)))
     (nth [this idx nf]
       (case idx
         0 a
         1 b
         nf))
     Associative
     (containsKey [this k]
       (case k
         0 true
         1 true
         false))
     (assoc [this idx v]
       (case idx
         0 (PairVector. v b meta)
         1 (PairVector. a v meta)
         (assoc (with-meta [a b] meta) idx v)))
     (entryAt [this idx]
       (case idx
         0 (MapEntry. idx a)
         1 (MapEntry. idx b)
         nil))
     IPersistentStack
     (peek [this] b)
     (pop [this] (with-meta [a] meta))
     Reversible
     (rseq [this] (rseq [a b]))
     ILookup
     (valAt [this idx]
       (nth this idx))
     (valAt [this idx nf]
       (nth this idx nf)))
   :cljs
   (comment
     ;TODO implement if needed
     ))


(defn pair-vector [a b]
  #?(:clj  (->PairVector a b nil)
     :cljs (vector a b)))
