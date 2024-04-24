(ns org.zsxf.experimental.dataflow
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.xf :as dbsp-xf]
            [org.zsxf.zset :as zs]
            [pangloss.transducers :as pxf]
            [taoensso.timbre :as timbre]
            [org.zsxf.experimental.data :as exp-data]))

(defonce *join-state (atom {}))
(defonce *grouped-by-state-team (atom {}))
(defonce *grouped-by-state-player (atom {}))

(def pipeline-xf
  (comp
    (mapcat identity)
    (pxf/branch
      ;:team/id
      (comp
        (map (fn [m] (timbre/info "team!") m))
        (map (fn [m] (if (:id m) m {})))
        (pxf/cond-branch
          empty?
          (map identity)
          :id
          (comp
            (dbsp-xf/->index-xf :id)
            (map (fn [grouped-by-result]
                   (timbre/spy grouped-by-result)
                   (swap! *grouped-by-state-team
                     (fn [m]
                       (zs/indexed-zset+ m grouped-by-result))))))))
      (comp
        (map (fn [m] (timbre/info "player!") m))
        (map (fn [m] (if (:player/team m) m {})))
        (pxf/cond-branch
          empty?
          (map identity)
          :player/team
          (comp
            (dbsp-xf/->index-xf :player/team)
            (map (fn [grouped-by-result]
                   (timbre/spy grouped-by-result)
                   (swap! *grouped-by-state-player
                     (fn [m]
                       (zs/indexed-zset+ m grouped-by-result)))))))))
    ;TODO join indexed-zsets
    (map (fn [j]
           (timbre/spy j)
           ))))

(defonce *state (atom nil))

(defn reset-pipeline []
  (reset! *join-state {})
  (reset! *grouped-by-state-team {})
  (reset! *grouped-by-state-player {})
  (let [from (a/chan 1)
        to   (a/chan (a/sliding-buffer 1)
               (map (fn [to-final] (timbre/spy to-final))))]
    (a/pipeline 1
      to
      pipeline-xf
      from)
    (reset! *state [from to])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn init []
  (let [[from to] @*state]
    ;(a/>!! from (exp-data/data1))
    ;(a/>!! from (exp-data/data2))
    (a/>!! from
      (zs/zset
        [
         ;{:team/id 3 :team/name "T3"}
         {:id 4 :team "A-dupe"}
         ;{:id 5 :team "B"}
         ;{:player-name "BP" :player/team 5}
         ;{:player-name "A1" :player/team 4}
         ;{:player-name "A2" :player/team 4}
         ]))))

(defn init-remove []
  (let [[from to] @*state]
    ;(a/>!! from (exp-data/data1))
    ;(a/>!! from (exp-data/data2))
    (a/>!! from
      (zs/zset-negative
        [
         {:id 4 :team "A-dupe"}
         ]))))

(comment
  (set! *print-meta* false)
  (set! *print-meta* true)
  (reset-pipeline)
  (init)
  (clojure.pprint/pprint
    @*grouped-by-state-team)
  (clojure.pprint/pprint
    @*grouped-by-state-player)
  (clojure.pprint/pprint
    (zs/join @*grouped-by-state-team @*grouped-by-state-player))
  (clojure.pprint/pprint
    @*join-state)

  (init-remove)
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
