(ns org.zsxf.experimental.ref-vs-atom
  (:require [org.zsxf.zset :as zs]))


(defonce *zset-atom-1 (atom #{}))

(defonce *zset-ref-1 (ref #{}))

(defn commutative-change [zs]
  (zs/zset+ zs #{^#:zset{:w 1} {:name "Alice"}}))

(defn change-atom! []
  (swap! *zset-atom-1 commutative-change))

(defn change-ref-with-commute! []
  (dosync
    (commute *zset-ref-1 commutative-change)))

(defn change-ref-with-alter! []
  (dosync
    (alter *zset-ref-1 commutative-change)))

(comment

  (run!
    (fn [_]
      (future (change-atom!)))
    (range 1000000))
  @*zset-atom-1

  (run!
    (fn [_]
      (future (change-ref-with-commute!)))
    (range 1000000))
  @*zset-ref-1

  (run!
    (fn [_]
      (future (change-ref-with-alter!)))
    (range 1000000))
  @*zset-ref-1

  ;TLDR atoms with clojure.core/swap! appear to be faster than refs even if we're using clojure.core/commute
  )
