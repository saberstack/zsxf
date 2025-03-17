(ns org.zsxf.xf
  (:require [net.cgrand.xforms :as xforms]
            [org.zsxf.zset :as zs]
            [pangloss.transducers :as pxf]
            [taoensso.timbre :as timbre]))

(defn zset-w [m]
  (timbre/spy
    (:zset/w (meta m) 0)))

(defn ->where-xf
  "SQL WHERE (aka filter), as per https://www.feldera.com/blog/SQL-on-Zsets/#filtering-sql-where
  This is different from regular Clojure filter in that it returns an empty set if the predicate is false.
  DBSP requires all operators (in our case, transducers) to return a zset rather than skipping the item."
  [pred]
  (map (fn [x]
         (if (pred x) x #{}))))

(defn ->dbsp-result-xf!
  "Save DBSP result"
  [an-atom]
  (map (fn [dbsp-result]
         ;(timbre/spy dbsp-result)
         (reset! an-atom dbsp-result))))

(defn ->index-xf
  [kfn]
  (xforms/by-key
    kfn
    (fn [m] m)
    (fn [k ms]
      ;(timbre/spy k)
      ;(timbre/spy ms)
      (if k {k ms} {}))
    (xforms/into #{})))

(defn count-xf
  "DBSP SQL COUNT"
  ([rf]
   (let [n (atom 0)]
     (fn
       ([] (rf))
       ([acc] (rf (unreduced (rf acc @n))))
       ([acc item]
        (timbre/spy item)
        (rf acc (swap! n (fn [prev-n] (+ prev-n (zset-w item))))) acc)))))

(defn for-xf [a-set]
  (xforms/for
    [x % y a-set] [x y]))

(defn join-xf
  [cond-pred-1 index-k-1 index-state-1 cond-pred-2 index-k-2 index-state-2]
  (let [index-state-1-prev @index-state-1
        index-state-2-prev @index-state-2]
    (comp
      ;receives a zset of maps
      (mapcat identity)
      ;receives a map (with zset weight)
      (pxf/branch
        ;join branch 1
        (comp
          (pxf/cond-branch
            cond-pred-1
            (comp
              (map (fn [m] (timbre/spy #{m})))              ;put each map back into a set so we can zset+ it
              (xforms/reduce zs/zset+)                      ;zset+ all the items
              (map (fn [zset] (zs/index zset index-k-1))))))
        ;join branch 2
        (comp
          ;:player/team
          (pxf/cond-branch
            cond-pred-2
            (comp
              (map (fn [m] #{m}))                           ;put each map back into a set so we can zset+ it
              (xforms/reduce zs/zset+)                      ;zset+ all the items
              (map (fn [zset] (zs/index zset index-k-2)))))))
      (partition-all 2)
      (map (fn [[delta-1 delta-2 :as v]]
             ;advance player and team indices
             (swap! index-state-1 (fn [m] (zs/indexed-zset-pos+ m delta-1)))
             (swap! index-state-2 (fn [m] (zs/indexed-zset-pos+ m delta-2)))
             v))
      (map (fn [[delta-1 delta-2]]
             (timbre/spy delta-1)
             (timbre/spy delta-2)
             (zs/indexed-zset+
               ;ΔTeam ⋈ Players
               (timbre/spy (zs/join-indexed* delta-1 index-state-2-prev))
               ;Teams ⋈ ΔPlayers
               (timbre/spy (zs/join-indexed* index-state-1-prev delta-2))
               ;ΔTeams ⋈ ΔPlayers
               (timbre/spy (zs/join-indexed* delta-1 delta-2)))))
      (map (fn [final-delta] (zs/indexed-zset->zset final-delta))))))

(defn mapcat-zset-tx
  "Receives a transaction represented by a vectors of zsets.
  Returns zsets one by one"
  []
  (mapcat (fn [tx-v] (timbre/spy tx-v))))

(defn query-result-set-xf [result-set-state]
  (map (fn [delta]
         (swap! result-set-state
           (fn [m] (zs/zset-pos+ m delta))))))
