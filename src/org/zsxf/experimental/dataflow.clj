(ns org.zsxf.experimental.dataflow
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
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

(defonce *result-set-state (atom {}))
(defonce *index-state-1 (atom {}))
(defonce *index-state-2 (atom {}))

(defn incremental-computation-xf
  "Equivalent SQL join:

  SELECT * FROM zsxf.team t
  JOIN zsxf.player p ON t.id = p.team
  WHERE t.id = 20
  "
  []
  (comp
    (mapcat identity)
    (map (fn [current-value]
           (timbre/spy current-value)))
    (pxf/branch
      ;:team/id (or :team/name, a unique attribute)
      ;branch
      (comp
        (map (fn [branch-m] (timbre/spy branch-m)))
        (map (fn [m] (if (:team/id m) m {})))
        (map (fn [branch-m] (timbre/spy branch-m)))
        (pxf/cond-branch
          :team/id
          (comp
            ;(dbsp-xf/->where-xf (fn [m] (= 20 (:team/id m))))
            (dbsp-xf/->index-xf :team/id)
            ;atoms
            (map (fn [grouped-by-result]
                   (swap! *grouped-by-state-team
                     (fn [m] (zs/indexed-zset+ m grouped-by-result)))
                   (timbre/spy
                     grouped-by-result))))))
      ;branch
      (comp
        ;:player/team
        (map (fn [branch-m] (timbre/spy branch-m)))
        (map (fn [m] (if (:player/team m) m {})))
        (map (fn [branch-m] (timbre/spy branch-m)))
        (pxf/cond-branch
          :player/team
          (comp
            (dbsp-xf/->index-xf :player/team)
            ;atoms
            (map (fn [grouped-by-result]
                   (swap! *grouped-by-state-player
                     (fn [m] (zs/indexed-zset+ m grouped-by-result)))
                   (timbre/spy grouped-by-result)))))))
    (map (fn [branch-result] (timbre/spy branch-result)))
    (map (fn [join-output]
           ;(timbre/spy [(count join-output) join-output])
           join-output))
    ;(xforms/reduce conj [])
    ))

(defn incremental-computation-xf-2
  [index-state-1 index-state-2]
  (let [index-state-1-prev @index-state-1
        index-state-2-prev @index-state-2]
    (comp
      (mapcat identity)                                     ;receives a vector representing transaction
      (mapcat identity)                                     ;receives a zset of maps
      (pxf/branch
        ;join branch 1
        (comp
          (pxf/cond-branch
            :team/id
            (comp
              (map (fn [m] #{m}))                           ;put each map back into a set so we can zset+ it
              (xforms/reduce zs/zset+)                      ;zset+ all the items
              (map (fn [zset] (zs/index zset :team/id)))))
          )
        ;join branch 2
        (comp
          ;:player/team
          (pxf/cond-branch
            :player/team
            (comp
              (map (fn [m] #{m}))                           ;put each map back into a set so we can zset+ it
              (xforms/reduce zs/zset+)                      ;zset+ all the items
              (map (fn [zset] (zs/index zset :player/team)))))
          ))
      (partition-all 2)
      (map (fn [[delta-1 delta-2 :as v]]
             ;advance player and team indices
             (swap! index-state-1 (fn [m] (zs/indexed-zset+ m delta-1)))
             (swap! index-state-2 (fn [m] (zs/indexed-zset+ m delta-2)))
             v))
      (map (fn [[delta-1 delta-2]]
             (timbre/spy delta-1)
             (timbre/spy delta-2)
             (zs/indexed-zset+
               ;ΔPlayers ⋈ Teams
               (timbre/spy (zs/join delta-1 index-state-2-prev))
               ;ΔTeams ⋈ Players
               (timbre/spy (zs/join index-state-1-prev delta-2))
               ; ΔPlayers ⋈ ΔTeams
               (timbre/spy (zs/join delta-1 delta-2)))))
      (map (fn [final-delta]
             (timbre/spy final-delta)))
      )))

(defn reset-index-state! []
  (reset! *index-state-1 {})
  (reset! *index-state-2 {})
  (reset! *result-set-state {}))

(defn query-result-set-xf [result-set-state]
  (map (fn [delta]
         delta
         (swap! result-set-state
           (fn [m] (zs/indexed-zset+ m delta))))))

(comment
  (reset-index-state!)
  ;transaction example
  (into []
    (comp
      (incremental-computation-xf-2 *index-state-1 *index-state-2)
      (query-result-set-xf *result-set-state))
    ;trivial
    [
     [(zs/zset
        [{:team/id 1 :team/name "T1"}])]]
    ;more involved
    #_[
     ;1
     [(zs/zset
        [{:team/name "T1" :team/id 1}
         {:team/name "T2" :team/id 2}
         {:player/team 1}])
      (zs/zset-negative
        [{:team/name "T1" :team/id 1}
         {:team/name "T2" :team/id 2}
         {:player/team 1}])]])

  (into []
    (comp
      (incremental-computation-xf-2 *index-state-1 *index-state-2)
      (query-result-set-xf *result-set-state))
    ;trivial
    [
     [(zs/zset
        [{:player/team 1 :player/name "P1"}])]])

  (into []
    (comp
      (incremental-computation-xf-2 *index-state-1 *index-state-2)
      (query-result-set-xf *result-set-state))
    ;trivial
    [
     [(zs/zset-negative
        [{:team/id 1 :team/name "T1"}])]])
  (do
    (timbre/spy @*index-state-1)
    (timbre/spy @*index-state-2)
    (timbre/spy @*result-set-state)))

; At every time step...
; Streaming incremental join update
; ΔPlayers ⋈ Teams + ΔPlayers ⋈ ΔTeams + Players ⋈ ΔTeams (update, to be summed with result set)
; vs...
; Streaming non-incremental join

(comment
  ;Delete poc
  (let [tx-1 (zs/join
               (zs/index
                 (zs/zset [{:team/name "T1" :team/id 1}])
                 :team/id)
               (zs/index
                 (zs/zset [{:player/name "P1" :player/team 1}])
                 :player/team))
        tx-2 (zs/join
               (zs/index
                 (zs/zset [{:team/name "T1" :team/id 1}])
                 :team/id)
               (zs/index
                 (zs/zset-negative [{:player/name "P1" :player/team 1}])
                 :player/team))]
    (zs/indexed-zset+ tx-1 tx-2)))

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
               (map (fn [to-final]
                      (timbre/spy to-final))))]
    (a/pipeline 3
      to
      (incremental-computation-xf)
      from)
    (reset! *state [from to])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

#_(defn init-from-memory []
    (let [[from to] @*state]
      (a/>!! from
        (zs/zset
          [
           {:team/id 4 :team/name "T3"}
           {:player-name "BP" :player/team 5}
           {:player-name "A1" :player/team 4}
           {:player-name "A2" :player/team 4}
           ]))))

(defn init-from-memory []
  (let [[from to] @*state]
    (a/>!! from
      (zs/zset
        [{:team/name "T1" :team/id 2}
         {:player/name "P1" :player/team 1}
         {:player/name "P2" :player/team 2}
         {:player/name "P3" :player/team 3}
         {:player/name "P4" :player/team 4}
         {:player/name "P5" :player/team 5}
         {:player/name "P6" :player/team 6}
         {:player/name "P7" :player/team 2}
         ]))

    #_(a/>!! from
        (zs/zset
          [
           {:player/name "P3" :player/team 1}
           {:player/name "P4" :player/team 1}
           ]))))

(defn init-remove []
  (let [[from to] @*state]
    (a/>!! from
      (zs/zset-negative
        [
         #:team{:name "T1", :id 2}
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
