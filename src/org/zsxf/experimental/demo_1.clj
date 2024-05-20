(ns org.zsxf.experimental.demo-1
  (:require [taoensso.timbre :as timbre]
            [tea-time.core :as tt]))

(defonce *refresh-data-task (atom nil))

(defn refresh-data!
  "Auto refresh data from database.
  Only for demo purpose."
  []
  ;TODO Implement
  (timbre/info ".."))

(defn start-refresh-data-task! []
  (tt/start!)
  (when (nil? @*refresh-data-task)
    (reset! *refresh-data-task
      (tt/every! 2
        (bound-fn []
          (refresh-data!))))))

(defn cancel-refresh-data-task! []
  (tt/cancel! @*refresh-data-task)
  (reset! *refresh-data-task nil))
