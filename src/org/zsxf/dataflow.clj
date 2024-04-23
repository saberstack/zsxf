(ns org.zsxf.dataflow
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as x]
            [org.zsxf.zset :as zs]
            [taoensso.timbre :as timbre]
            [ss.loop :as ss.loop]
            [pangloss.transducers :as pxf]))

(defonce index-1-atom (atom {}))
(defonce index-2-atom (atom {}))

(defn init []
  (let [join-chan     (a/chan 2
                        (comp
                          (partition-all 2)
                          (map (fn [[m1 m2]]
                                 (timbre/spy (Thread/currentThread))
                                 (timbre/spy [m1 m2])))))
        index-1-chan  (a/chan 1
                        (comp
                          (map (fn [m]
                                 (group-by :team/id [m])))
                          (map (fn [m] (a/put! join-chan m)))))

        index-2-chan  (a/chan 1
                        (comp
                          (comp
                            (map (fn [m]
                                   (group-by :person/team [m])))
                            (map (fn [m] (a/put! join-chan m))))))

        filter-1-chan (a/chan 1
                        (comp
                          (filter (fn [m]
                                    (timbre/spy (Thread/currentThread))
                                    (clojure.string/starts-with? (:team/name m) "A")))
                          (map (fn [m]
                                 (timbre/spy (Thread/currentThread))
                                 (a/put! index-1-chan m)))))
        entry-1-chan  (a/chan 1
                        (map (fn [m]
                               (a/put! filter-1-chan m))))
        entry-2-chan  (a/chan 1
                        (map (fn [m]
                               (timbre/spy (Thread/currentThread))
                               (a/put! index-2-chan m))))]
    (a/>!! entry-1-chan {:team/name "Angels" :team/id 1})
    (a/>!! entry-2-chan {:person/uuid "Charles" :person/team 1})))




;1. sequence of z-sets (one seq per transaction)
;2. dataflow of core.async loops
; or
;2. dataflow of transducers
;3. pipeline which receives vectors of one or many z-sets
; (pipeline n to xf from)


(defn pipelined []
  (let [from (a/chan 42)]
    (a/pipeline 8
      (a/chan 42)
      (comp
        (mapcat identity)
        (map (fn [z-set] (timbre/spy z-set))))
      from)
    from))

(defn multi-xf []
  ;WIP
  (into
    []
    #_(x/transjuxt
        {:player/team (x/by-key :player/team (x/reduce conj))
         :team/id     (x/by-key :team/id (x/reduce conj))})
    (comp
      (pxf/cond-branch
        :team/id (map (fn [m] (assoc m :info "team!")))
        :player/team (map (fn [m] (assoc m :info "player!"))))
      (map (fn [& xs] (timbre/spy xs))))
    [{:team/id 1 :team/name "SF"}
     {:team/id 2 :team/name "NY"}
     {:player/name "Alice" :player/team 1}
     {:player/name "Bob" :player/team 2}]))

(defn pipeline-data []
  (zs/zset
    [{:team/id 1 :team/name "SF"}
     {:team/id 2 :team/name "NY"}
     {:player/name "Alice" :player/team 1}
     {:player/name "Bob" :player/team 2}
     {:player/name "Chris" :player/team 2}]))

(defn pipeline-data-2 []
  (zs/zset
    [{:team/id 3 :team/name "LA"}
     {:team/id 4 :team/name "AU"}
     {:player/name "A" :player/team 3}
     {:player/name "B" :player/team 3}
     {:player/name "C" :player/team 4}]))

(def pipeline-2-xf
  (comp
    (mapcat identity)
    (pxf/cond-branch
      :team/id
      (comp
        (map (fn [m]
               (timbre/info "team!")
               m))
        (pxf/grouped-by :team/id :on-value set))
      :player/team
      (comp
        (map (fn [m]
               (timbre/info "player!")
               m))
        (pxf/grouped-by :player/team :on-value set)))
    ;TODO implement indexed-zset add and multiply
    (map (fn [grouped-by-result] (timbre/spy grouped-by-result)))))

(defonce tmp-1 (atom nil))

(defn reset-pipeline []
  (let [from (a/chan 1)
        to   (a/chan (a/sliding-buffer 1)
               (map (fn [to-final] (timbre/spy to-final))))]
    (a/pipeline 1
      to
      pipeline-2-xf
      from)
    (reset! tmp-1 [from to])))

(comment
  (into
    []
    pipeline-2-xf
    [(pipeline-data)])

  (let [[from to] @tmp-1]
    (a/>!! from (pipeline-data))
    (a/>!! from (pipeline-data-2))
    (timbre/spy (a/<!! to))
    (timbre/spy (a/<!! to))
    (timbre/spy (a/<!! to))
    (timbre/spy (a/<!! to)))

  (let [ch (a/chan 1
             pipeline-2-xf
             #_(pxf/cond-branch
                 :team/id
                 (comp
                   (pxf/grouped-by :team/id)
                   (map (fn [grouped-by] (timbre/spy grouped-by))))))]
    (a/>!! ch {:team/id 1 :name "A"})
    ;(a/>!! ch {:team/id 1 :name "B"})
    ;(a/>!! ch {:team/id 1 :name "C"})
    ;(a/close! ch)
    (do
      (timbre/spy (a/<!! ch))
      ;(timbre/spy (a/<!! ch))
      ;(timbre/spy (a/<!! ch))
      )
    )

  )


(defn go-loop-test []
  (ss.loop/go-loop []
    (a/<! (a/timeout 1000))
    (println "Loop 1")
    (recur)))

(def mix-out-1 (a/chan 42 (map (fn [x]
                                 (condp = x
                                   :a "Got an :a!"
                                   :b "Got a :b!"
                                   "Got unknown value")))))
(def mix-1 (a/mix mix-out-1))
(def mix-in-1 (a/chan 42))
(comment
  (a/admix mix-1 mix-in-1)
  (a/>!! mix-in-1 :a)
  (a/<!! mix-out-1)
  (a/>!! mix-in-1 :aaa)
  (a/<!! mix-out-1))

(comment
  (->
    #{{:team/name "Angels" :team/id 1}}
    (zs/zset)
    (zs/zset+ (zs/zset #{{:team/country "Italy"}}))
    (zs/zset+ (zs/zset #{{:team/name "Angels" :team/id 1}})))




  (zs/indexed-zset
    (zs/zset
      #{{:team/name "Angels" :team/id 1}})
    :team/id)

  (zs/zset
    #{{:player/name "Alice" :team/id 1}})

  )

;JOIN Output contents vary depending on the upstream/parent requirements in the dataflow tree
; - logically, the output of a JOIN is always the same: the tuples that match
;PROBLEM: how to do queries without feeding all changes since the start of time?
;
(defn set-join []
  (clojure.set/join
    [{:team/id 1 :team/name "SF"}
     {:team/id 2 :team/name "NY"}]
    [{:player/name "Alice" :player/team 1}
     {:player/name "Bob" :player/team 2}]
    {:team/id :player/team}))
