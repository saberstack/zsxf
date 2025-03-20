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
  [pred-1 index-k-1 index-state-1 pred-2 index-k-2 index-state-2]
  (comp
    ;receives a zset of items
    (mapcat identity)
    ;receives a zset item (with zset weight)
    (map (fn [join-xf-item] (timbre/spy join-xf-item)))
    (map (fn [zset-item]
           (cond
             (pred-1 zset-item) [(zs/index #{zset-item} index-k-1) {} #{}]
             (pred-2 zset-item) [{} (zs/index #{zset-item} index-k-2) #{}]
             :else [{} {} #{zset-item}])))
    (pxf/cond-branch
      ;does this xf care about the current item?
      (fn [[delta-1 delta-2  _zset :as delta-1+delta-2+zset]]
        (timbre/spy delta-1+delta-2+zset)
        ;if none of the predicates were true...
        (and (empty? delta-1) (empty? delta-2)))
      (map (fn [[_delta-1 _delta-2 zset]]
             (timbre/info "passing through...")
             (timbre/spy zset)))
      ;else, proceed to join
      any?
      (comp
        (map (fn [[delta-1 delta-2 :as v]]
               (let [index-state-1-prev @index-state-1
                     index-state-2-prev @index-state-2]
                 ;advance player and team indices
                 (swap! index-state-1 (fn [m] (timbre/spy (zs/indexed-zset-pos+ m delta-1))))
                 (swap! index-state-2 (fn [m] (timbre/spy (zs/indexed-zset-pos+ m delta-2))))
                 ;return delta
                 [index-state-1-prev index-state-2-prev [delta-1 delta-2]])))
        (map (fn [[index-state-1-prev index-state-2-prev [delta-1 delta-2]]]
               (timbre/spy [delta-1 index-state-2-prev])
               (timbre/spy [index-state-1-prev delta-2])
               (zs/indexed-zset+
                 ;ΔTeam ⋈ Players
                 (timbre/spy (zs/join-indexed* delta-1 index-state-2-prev))
                 ;Teams ⋈ ΔPlayers
                 (timbre/spy (zs/join-indexed* index-state-1-prev delta-2))
                 ;ΔTeams ⋈ ΔPlayers
                 (timbre/spy (zs/join-indexed* delta-1 delta-2)))))
        (map (fn [join-xf-delta]
               (timbre/spy (zs/indexed-zset->zset join-xf-delta))))))))

(defn mapcat-zset-transaction-xf
  "Receives a transaction represented by a vectors of zsets.
  Returns zsets one by one"
  []
  (mapcat (fn [tx-v] (timbre/spy tx-v))))

(defn query-result-set-xf [result-set-state]
  (map (fn [result-set-delta]
         (timbre/spy result-set-delta)
         (swap! result-set-state
           (fn [m] (zs/zset-pos+ m result-set-delta))))))

(defn disj-irrelevant-items [zset & preds]
  (transduce
    (filter (apply some-fn preds))
    conj
    #{}
    zset))
