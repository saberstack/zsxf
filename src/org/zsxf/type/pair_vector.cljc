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
     (equiv [this other] (.equals [a b] other))
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
       (cond
         (zero? idx) (PairVector. v b meta)
         (= 1 idx) (PairVector. a v meta)
         :else (assoc (with-meta [a b] meta) idx v)))
     Indexed
     (nth [this idx]
       (cond
         (zero? idx) a
         (= 1 idx) b
         :else (nth [a b] idx)))
     (nth [this idx nf]
       (cond
         (zero? idx) a
         (= 1 idx) b
         :else nf))
     Associative
     (containsKey [this k]
       (cond
         (zero? k) true
         (= 1 k) true
         :else false))
     (assoc [this idx v]
       (cond
         (zero? idx) (PairVector. v b meta)
         (= 1 idx) (PairVector. a v meta)
         :else (assoc (with-meta [a b] meta) idx v)))
     (entryAt [this idx]
       (cond
         (zero? idx) (MapEntry. idx a)
         (= 1 idx) (MapEntry. idx b)
         :else nil))
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
