(ns org.saberstack.performance.intake
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [ss.loop :as ss.loop]
            [org.saberstack.clock.monotonic :as monotonic]
            [taoensso.timbre :as timbre]))

(defn new-monitor []
  (let [ch (a/chan 42
                     (comp
                       (xforms/window-by-time
                         (fn [m] (/ (:ts m) 1e9))
                         1
                         (fn
                           ([] 0)
                           ([accum] accum)
                           ([accum m] (+ accum (m :n))))
                         (fn [accum m-inv]
                           (- accum (m-inv :n))))
                       (map (fn [windowed-cnt]
                              [(monotonic/now) windowed-cnt]))))]
    (ss.loop/go-loop
      []
      (let [_ (a/>! ch {:ts (monotonic/now) :n 0})
            _ (a/<! (a/timeout 500))
            intake-data (a/poll! ch)]
        (when intake-data
          (timbre/spy intake-data))
        (recur)))
    ch))


(comment

  (def ch (new-monitor))

  (time
    @(future
       (run!
         (fn [n]
           (a/>!! ch {:ts (monotonic/now) :n (+ 500 (rand-int 500))}))
         (range 10000000))))

  (Thread/sleep 10000)
  (time
    (a/>!! ch {:ts (monotonic/now) :n 42}))

  (ss.loop/stop-all)
  )
