(ns org.zsxf.experimental.dataflow
  (:require [clojure.core.async :as a]
            [org.zsxf.jdbc.postgres :as postgres]
            [org.zsxf.xf :as dbsp-xf]
            [org.zsxf.zset :as zs]
            [pangloss.transducers :as pxf]
            [taoensso.timbre :as timbre]))

(defonce *grouped-by-state-team (atom {}))
(defonce *grouped-by-state-player (atom {}))

(defonce *grouped-by-state-team-2 (ref {}))
(defonce *grouped-by-state-player-2 (ref {}))

(defonce *grouped-by-state-team-3 (agent {}))
(defonce *grouped-by-state-player-3 (agent {}))

(defn incremental-computation-xf
  "Equivalent SQL join:

  SELECT * FROM zsxf.team t
  JOIN zsxf.player p ON t.id = p.team
  WHERE t.id = 20
  "
  []
  (comp
    (mapcat identity)
    (pxf/branch
      ;:team/id (or :team/name, a unique attribute)
      (comp
        (map (fn [m] m))
        (map (fn [m] (if (:team/id m) m {})))
        (pxf/cond-branch
          empty?
          (map identity)
          :team/id
          (comp
            (dbsp-xf/->where-xf (fn [m] (= 20 (:team/id m))))
            (dbsp-xf/->index-xf :team/id)
            ;atoms
            (map (fn [grouped-by-result]
                     (swap! *grouped-by-state-team
                       (fn [m] (zs/indexed-zset+ m grouped-by-result))))))))
      (comp
        ;:player/team
        (map (fn [m] m))
        (map (fn [m] (if (:player/team m) m {})))
        (pxf/cond-branch
          empty?
          (map identity)
          :player/team
          (comp
            (dbsp-xf/->index-xf :player/team)
            ;atoms
            (map (fn [grouped-by-result]
                   (swap! *grouped-by-state-player
                     (fn [m] (zs/indexed-zset+ m grouped-by-result)))))))))
    (map (fn [j] j))))

(defonce *state (atom nil))

(defn reset-pipeline! []
  (reset! *grouped-by-state-team {})
  (reset! *grouped-by-state-player {})
  (send *grouped-by-state-team-3 (fn [_] {}))
  (send *grouped-by-state-player-3 (fn [_] {}))

  (dosync
    (alter *grouped-by-state-player-2 (fn [_] {}))
    (alter *grouped-by-state-team-2 (fn [_] {})))

  (let [from (a/chan 42)
        to   (a/chan (a/sliding-buffer 1)
               (map (fn [to-final] to-final)))]
    (a/pipeline 3
      to
      (incremental-computation-xf)
      from)
    (reset! *state [from to])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn init-from-memory []
  (let [[from to] @*state]
    (a/>!! from
      (zs/zset
        [
         ;{:team/id 3 :team/name "T3"}
         {:id 4 :team "A-dupe"}
         ;{:id 5 :team "B"}
         ;{:player-name "BP" :player/team 5}
         {:player-name "A1" :player/team 4}
         ;{:player-name "A2" :player/team 4}
         ]))))

(defn init-remove []
  (let [[from to] @*state]
    (a/>!! from
      (zs/zset-negative
        [
         {:id 4 :team "A-dupe"}
         ]))))

(def partition-postgres-data-xf
  (comp
    (partition-all 50000)
    (map (fn [sets]
           (apply clojure.set/union sets)))))

(defn init-from-postgres! []
  (reset-pipeline!)
  (let [[from to] @*state]
    (run!
      (fn [zset] (a/>!! from zset))
      ;partitioned
      (sequence
        partition-postgres-data-xf
        @postgres/*all-teams)
      ;non-partitioned
      #_@postgres/*all-teams))

  (let [[from to] @*state]
    (run!
      (fn [zset] (a/>!! from zset))
      ;partitioned
      (sequence
        partition-postgres-data-xf
        @postgres/*all-players)
      ;non-partitioned
      #_@postgres/*all-players)))

(defn incremental-from-postgres [zsets]
  (let [[from _to] @*state]
    (run!
      (fn [zset] (a/>!! from zset))
      zsets)))

(comment
  (set! *print-meta* false)
  (set! *print-meta* true)
  (reset-pipeline!)
  (init-from-memory)
  @*grouped-by-state-team
  @*grouped-by-state-player
  (zs/join @*grouped-by-state-team @*grouped-by-state-player)

  @*grouped-by-state-team-2
  @*grouped-by-state-player-2

  (zs/join @*grouped-by-state-team-2 @*grouped-by-state-player-2)

  )


; Scratch
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(comment

  (zs/indexed-zset*
    (zs/index
      (zs/zset [{:team "A" :id 1} {:team "Aa" :id 1} {:team "B" :id 2}])
      :id)
    (zs/index
      (zs/zset [{:player "R" :team 1} {:player "S" :team 2} {:player "T" :team 3}])
      :team))

  )
