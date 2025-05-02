(ns org.zsxf.experimental.hash-map
  (:require [clj-memory-meter.core :as mm])
  (:import (java.util HashMap)))


(defn init-java-hash-map [n kfn]
  (let [^HashMap o (HashMap.)]
    (loop [idx 0]
      (if (= n idx)
        o
        (do
          (.put o (kfn idx) idx)
          (recur (inc idx)))))))

(defn init-clj-hash-map [n kfn]
  (transduce
    (comp
      (take n)
      (map-indexed (fn [idx x] [(kfn idx) idx])))
    conj
    {}
    (repeatedly +)))


(comment
  (init-clj-hash-map 10 identity)
  (init-java-hash-map 10 identity))


(defn run-benchmarks [n]
  ;TODO improve by printing as data/table format
  (mm/measure
    (time
      (init-java-hash-map n identity)))

  (mm/measure
    (time
      (init-clj-hash-map n identity))))

(comment

  (run-benchmarks 1000000))
