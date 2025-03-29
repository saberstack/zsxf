(ns org.zsxf.archive.experimental.core
  (:require [net.cgrand.xforms :as xforms]
            [org.zsxf.zset :as zs]))

; Basic implementation of the ideas presented in
; DBSP: Automatic Incremental View Maintenance for Rich Query Languages
; https://www.youtube.com/watch?v=J4uqlG1mtbU

(defn stream->stream
  [s]
  (sequence
    (map +)
    [1 2 3]))

(defn streams->stream
  ""
  [& ss]
  (apply sequence
    (map +)
    ss))

(defn delay-xf
  "Delay the input by one item"
  []
  (fn [rf]
    (let [prev-input (atom nil)]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result input]
         (let [prev-input' @prev-input]
           (reset! prev-input input)
           (if (some? prev-input')
             (rf result prev-input')
             (rf result 0))))))))

(defn changes
  "Returns a stream of changes in a stream of numbers"
  [s]
  (sequence
    (map -)
    s
    (sequence (delay-xf) s)))

(defn changes-xf []
  (xforms/window 2
    (fn
      ([] 0)
      ([x] x)
      ([x y]
       (- y x)))
    +))

(defn integration-xf
  ([] (integration-xf zs/zset+))
  ([f]
   (comp
     (xforms/reductions f)
     (drop 1))))

(defn changes-2
  [s]
  (sequence
    (changes-xf)
    s))

(defn integration
  [s]
  (sequence
    (integration-xf)
    s))
