(ns org.zsxf.experimental.demo-1
  (:require [taoensso.timbre :as timbre]
            [org.zsxf.jdbc.postgres :as postgres]
            [tea-time.core :as tt]))

(defonce *refresh-data-task (atom nil))

(defn refresh-data!
  "Auto refresh data from database.
  Only for demo purpose."
  []
  ;TODO Implement
  (postgres/init-all-data)
  (timbre/info "Data refreshed"))

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
