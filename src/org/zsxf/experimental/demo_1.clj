(ns org.zsxf.experimental.demo-1
  (:require [taoensso.timbre :as timbre]
            [org.zsxf.jdbc.postgres :as postgres]
            [org.zsxf.experimental.dataflow :as xp-dataflow]
            [tea-time.core :as tt]))

(defonce *refresh-data-task (atom nil))

(defn refresh-data!
  "Auto refresh data from database.
  Only for demo purpose."
  []
  ;TODO Implement
  (time
    (postgres/init-all-data))
  (timbre/info "Data refreshed"))

(defn start-refresh-data-task! []
  (tt/start!)
  (when (nil? @*refresh-data-task)
    (reset! *refresh-data-task
      (tt/every! 10
        (bound-fn []
          (refresh-data!))))))

(defn cancel-refresh-data-task! []
  (tt/cancel! @*refresh-data-task)
  (reset! *refresh-data-task nil))

(defn run-demo! []
  (postgres/init-all-data)
  (xp-dataflow/init-from-postgres!))

(defn count-grouped-by-state-player []
  (apply +
    (vals
      (update-vals @xp-dataflow/*grouped-by-state-player count))))

(defn count-grouped-by-state-player-2 []
  (apply +
    (vals
      (update-vals @xp-dataflow/*grouped-by-state-player-2 count))))
