(ns org.zsxf.archive.experimental.dataflow
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.archive.jdbc.postgres :as postgres]
            [org.zsxf.zset :as zset]
            [org.zsxf.zset :as zs]
            [pangloss.transducers :as pxf]
            [taoensso.timbre :as timbre]))

(defonce *grouped-by-state-team (atom {}))
(defonce *grouped-by-state-player (atom {}))

(defonce *grouped-by-state-team-2 (ref {}))
(defonce *grouped-by-state-player-2 (ref {}))

(defonce *grouped-by-state-team-3 (agent {}))
(defonce *grouped-by-state-player-3 (agent {}))

(defn join
  "Join two indexed zsets as a map relation. Does not multiply zsets."
  [indexed-zset-1 indexed-zset-2]
  (let [commons (clojure.set/intersection (set (keys indexed-zset-1)) (set (keys indexed-zset-2)))]
    (transduce
      (map (fn [common] [(indexed-zset-1 common) (indexed-zset-2 common)]))
      conj
      {}
      commons)))

(defn ->index-xf
  [kfn]
  (xforms/by-key
    kfn
    (fn [m] m)
    (fn [k ms]
      (if k {k ms} {}))
    (xforms/into #{})))

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
            (->index-xf :team/id)
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
            (->index-xf :player/team)
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

; At every time step...
; Streaming incremental join update
; ΔPlayers ⋈ Teams + ΔPlayers ⋈ ΔTeams + Players ⋈ ΔTeams (update, to be summed with result set)
; vs...
; Streaming non-incremental join

(comment
  ;Delete poc
  (let [tx-1 (join
               (zs/index
                 (zs/zset [{:team/name "T1" :team/id 1}])
                 :team/id)
               (zs/index
                 (zs/zset [{:player/name "P1" :player/team 1}])
                 :player/team))
        tx-2 (join
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

(defn tx-datoms->zset [datoms]
  (transduce
    (map (fn [[e a v t add-or-retract]]
           (let [weight (condp = add-or-retract true 1 false -1)]
             (zset/zset-item {:db/id e a v} weight))))
    conj
    #{}
    datoms))

(comment
  (tx-datoms->zset
    [[4 :person/name "Alice" 536870917 true]
     [4 :person/team 1 536870917 true]
     [5 :person/name "Bob" 536870917 true]]))

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
  (join @*grouped-by-state-team @*grouped-by-state-player)

  @*grouped-by-state-team-2
  @*grouped-by-state-player-2

  (join @*grouped-by-state-team-2 @*grouped-by-state-player-2)

  )
