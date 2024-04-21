(ns org.zsxf.experimental.dataflow
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.zset :as zs]
            [pangloss.transducers :as pxf]
            [taoensso.timbre :as timbre]
            [org.zsxf.experimental.data :as exp-data]))

(defonce *join-state (atom {}))

(def pipeline-xf
  (comp
    (mapcat identity)
    (pxf/branch
      ;:team/id
      (comp
        (map (fn [m] (timbre/info "team!") m))
        (map (fn [m] (if (:team/id m) m {})))
        (pxf/cond-branch
          empty?
          (map identity)
          :team/id
          (xforms/by-key
            :team/id
            ;set
            ;vector
            ;(map identity)
            (fn [m] m)
            (fn [k ms] {k ms})
            (xforms/into #{}))))
      (comp
        (map (fn [m] (timbre/info "player!") m))
        (map (fn [m] (if (:player/team m) m {})))
        (pxf/cond-branch
          empty?
          (map identity)
          :player/team
          (xforms/by-key
            :player/team
            ;set
            ;vector
            ;(map identity)
            (fn [m] m)
            (fn [k ms] {k ms})
            (xforms/into #{})))))
    ;TODO implement indexed-zset add and multiply
    (map (fn [grouped-by-result]
           (timbre/spy grouped-by-result)
           (swap! *join-state
             (fn [m]
               (zs/indexed-zset* m grouped-by-result)))))))

(defonce *state (atom nil))

(defn reset-pipeline []
  (reset! *join-state {})
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
      (zs/->zset
        [{:team/id 3 :team/name "T3"}

         {:team/id 4 :team/name "T4"}
         {:player/name "A" :player/team 3}
         {:player/name "B" :player/team 3}
         ;{:player/name "C" :player/team 4}
         ]))))

(comment
  (reset-pipeline)
  (init)
  @*join-state
  )
