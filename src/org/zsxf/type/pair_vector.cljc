(ns org.zsxf.type.pair-vector)

(declare pair-vector)

(deftype PairVector [a b meta]
  clojure.lang.IObj
  (meta [this] meta)
  (withMeta [this m] (PairVector. a b m))
  clojure.lang.IPersistentCollection
  (empty [this] [])
  (equiv [this other] (.equals [a b] other))
  clojure.lang.Counted
  (count [this] 2)
  clojure.lang.Seqable
  (seq [this] (seq [a b]))
  clojure.lang.IPersistentVector
  (length [this] 2)
  (cons [this x] (conj [a b] x))
  (assocN [this idx v]
    (cond
      (zero? idx) (pair-vector v b)
      (= 1 idx) (pair-vector a v)
      :else (assoc [a b] idx v)))
  clojure.lang.Indexed
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
  clojure.lang.Associative
  (containsKey [this k]
    (cond
      (zero? k) true
      (= 1 k) true
      :else false))
  (assoc [this idx v]
    (cond
      (zero? idx) (pair-vector v b)
      (= 1 idx) (pair-vector a v)
      :else (assoc [a b] idx v)))
  (entryAt [this idx]
    (cond
      (zero? idx) (clojure.lang.MapEntry. idx a)
      (= 1 idx) (clojure.lang.MapEntry. idx b)
      :else nil))
  clojure.lang.IPersistentStack
  (peek [this] b)
  (pop [this] [a])
  clojure.lang.Reversible
  (rseq [this] (rseq [a b]))
  clojure.lang.ILookup
  (valAt [this idx]
    (nth this idx))
  (valAt [this idx nf]
    (nth this idx nf)))


(defn pair-vector [a b]
  (->PairVector a b nil))

(comment
  (pair-vector :a :b)
  )
