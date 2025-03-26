(ns org.zsxf.util
  (:require [clojure.core.async :as a])
  (:import (clojure.lang IReduceInit)))

(defn reducible->chan
  "Take the rows from the reducible and put them onto a channel. Return the channel.
  Useful for streaming a large number of rows from a database table without out-of-memory errors."
  [^IReduceInit reducible ch]
  (future
    (transduce
      (comp
        (map (fn [row] (a/>!! ch row)))
        ; halt when the receiving channel is full
        ; WARNING: core.async sliding-buffer and dropping-buffer will not halt
        ;(halt-when nil?)
        )
      conj
      []
      (eduction
        (map (fn [row] (into {} row)))
        reducible))
    (a/close! ch))
  ;return channel
  ch)

(defn fpred
  "Like fnil, but with a custom predicate"
  ([f pred x]
   (fn
     ([a] (f (if (pred a) x a)))
     ([a b] (f (if (pred a) x a) b))
     ([a b c] (f (if (pred a) x a) b c))
     ([a b c & ds] (apply f (if (pred a) x a) b c ds))))
  ([f pred x y]
   (fn
     ([a b] (f (if (pred a) x a) (if (pred b) y b)))
     ([a b c] (f (if (pred a) x a) (if (pred b) y b) c))
     ([a b c & ds] (apply f (if (pred a) x a) (if (pred b) y b) c ds))))
  ([f pred x y z]
   (fn
     ([a b] (f (if (pred a) x a) (if (pred b) y b)))
     ([a b c] (f (if (pred a) x a) (if (pred b) y b) (if (pred c) z c)))
     ([a b c & ds] (apply f (if (pred a) x a) (if (pred b) y b) (if (pred c) z c) ds)))))

(defn nth2
  "Like nth but doesn't throw"
  ([coll index]
   (nth2 coll index nil))
  ([coll index not-found]
   ((fpred nth (comp not vector?) nil) coll index not-found)))
