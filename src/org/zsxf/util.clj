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
