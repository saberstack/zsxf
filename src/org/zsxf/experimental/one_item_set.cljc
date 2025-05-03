(ns org.zsxf.experimental.one-item-set
  (:require [clj-memory-meter.core :as mm])
  (:import (clojure.lang IFn IPersistentCollection IPersistentSet Counted Seqable)))


(deftype OneItemSet [item]
  IPersistentSet
  (get [this x] (when (= item x) item))
  (contains [this x] (= item x))
  (disjoin [this x] (if (= item x) #{} this))
  IPersistentCollection
  (empty [this] #{})
  (cons [this x] (conj #{item} x))
  (equiv [this other] (= #{item} other))
  Counted
  (count [this] 1)
  Seqable
  (seq [this] (seq [item]))
  IFn
  (invoke [this x] (when (= item x) item)))


(comment
  (=
    (->OneItemSet 1)
    #{1})

  (mm/measure #{1})
  (mm/measure #{1})

  (mm/measure (conj (OneItemSet. 1) 2))

  (type (disj (->OneItemSet 1) 1))

  (empty (->OneItemSet 1))

  (contains? (->OneItemSet 1) 2)

  ((->OneItemSet 1) 1)
  )

(defn one-item-set [item]
  (->OneItemSet item))
