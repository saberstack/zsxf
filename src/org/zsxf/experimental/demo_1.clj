(ns org.zsxf.experimental.demo-1
  (:require [org.zsxf.zset :as zs]
            [taoensso.timbre :as timbre]
            [org.zsxf.jdbc.postgres :as postgres]
            [org.zsxf.experimental.dataflow :as xp-dataflow]
            [tea-time.core :as tt]
            [tech.v3.dataset :as ds]))

(defonce *refresh-data-task (atom nil))

(defn incremental-refresh-data!
  "Auto refresh data from database.
  Only for demo purpose."
  []
  (let [{:keys [new-players new-teams deleted-players deleted-teams]} (postgres/incremental-data)]
    ;(timbre/spy [new-players new-teams])
    ;(timbre/spy [deleted-players deleted-teams])
    (xp-dataflow/incremental-from-postgres
      (clojure.set/union
        new-players
        new-teams
        (into #{} (map zs/zset-negate) deleted-players)
        (into #{} (map zs/zset-negate) deleted-teams)))))

(defn start-refresh-data-task! []
  (tt/start!)
  (when (nil? @*refresh-data-task)
    (reset! *refresh-data-task
      (tt/every! 2
        (bound-fn []
          (incremental-refresh-data!))))))

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

(defn count-grouped-by-state-player-3 []
  (apply +
    (vals
      (update-vals @xp-dataflow/*grouped-by-state-player-3 count))))

(defn count-final-result []
  (update-vals
    (zs/join @xp-dataflow/*grouped-by-state-team @xp-dataflow/*grouped-by-state-player)
    count))

(defn final-result []
  (let [[k v]
        (first
          (zs/join @xp-dataflow/*grouped-by-state-team @xp-dataflow/*grouped-by-state-player))]
    (ds/sort-by-column
      (ds/->dataset
        (sequence
          (comp
            (map merge))
          (repeat (first k))
          v))
      :player/id
      >
      )))

(comment
  ;postgres deletion PoC
  (xp-dataflow/incremental-from-postgres
    (map zs/zset-negate
      #{#{^#:zset{:w 1} #:player{:id 24975004, :last_name "Player A1", :team 20}}}))
  )

; init 100,000 rows
; the refresh data task looks for id > 100,000

(comment
  (do
    (time
      (doall
        (sequence
          (comp
            ;(take 100)
            (partition-all 1000)
            (map (fn [sets]
                   (apply clojure.set/union sets))))
          @postgres/*all-players)))
    :done))
