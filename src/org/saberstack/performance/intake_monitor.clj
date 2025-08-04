(ns org.saberstack.performance.intake-monitor
  "Performance monitoring for data intake rates.
   Provides real-time windowed counters for tracking throughput over time windows.

   Alpha, subject to change."
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [ss.loop :as ss.loop]
            [org.saberstack.clock.monotonic :as monotonic]
            [taoensso.timbre :as timbre]))

(defn create-monitor
  "Creates a monitoring channel that tracks intake rates over 1-second windows."
  []
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
      ;; Send heartbeat with zero count to maintain window state
      (a/>! ch {:ts (monotonic/now) :n 0})
      ;; Wait between heartbeats
      (a/<! (a/timeout 500))
      (let [intake-data (a/poll! ch)]
        ;; Log windowed counts for observability
        ;; TODO allow other means of observing intake-data
        (when intake-data
          (timbre/spy intake-data))
        (recur)))
    ;; Return the channel
    ch))

(defn input
  "Records an intake event on the monitor channel.

   Args:
     monitor - channel returned by create-monitor
     n - count of items processed in this event
   Blocks until the event is accepted."
  ;TODO potentially add fire-and-forget option
  [monitor n]
  (a/>!! monitor {:ts (monotonic/now) :n n}))

(comment
  (ss.loop/stop-all))
