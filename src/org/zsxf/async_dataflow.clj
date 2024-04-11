(ns org.zsxf.async-dataflow
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [taoensso.timbre :as timbre]
            [ss.loop :as ss.loop]))

(defonce index-1-atom (atom {}))
(defonce index-2-atom (atom {}))

(defn init []
  (let [join-chan    (a/chan 2
                       (comp
                         (partition-all 2)
                         (map (fn [[m1 m2]]
                                (timbre/spy (Thread/currentThread))
                                (timbre/spy [m1 m2])))))
        index-1-chan (a/chan 1
                       (comp
                         (map (fn [m]
                                (group-by :team/id [m])))
                         (map (fn [m] (a/put! join-chan m)))))

        index-2-chan (a/chan 1
                       (comp
                         (comp
                           (map (fn [m]
                                  (group-by :person/team [m])))
                           (map (fn [m] (a/put! join-chan m))))))

        filter-1-chan  (a/chan 1
                       (comp
                         (filter (fn [m]
                                   (timbre/spy (Thread/currentThread))
                                   (clojure.string/starts-with? (:team/name m) "A")))
                         (map (fn [m]
                                (timbre/spy (Thread/currentThread))
                                (a/put! index-1-chan m)))))
        entry-1-chan (a/chan 1
                       (map (fn [m]
                              (a/put! filter-1-chan m))))
        entry-2-chan (a/chan 1
                       (map (fn [m]
                              (timbre/spy (Thread/currentThread))
                              (a/put! index-2-chan m))))]
    (a/>!! entry-1-chan {:team/name "Angels" :team/id 1})
    (a/>!! entry-2-chan {:person/uuid "Charles" :person/team 1})))


(defn pipelined [])

;1. sequence of z-sets (one seq per transaction)
;2. dataflow of core.async loops
; or
;2. dataflow of transducers

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
