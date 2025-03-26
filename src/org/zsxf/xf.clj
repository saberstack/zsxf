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


(defn for-xf [a-set]
  (xforms/for
    [x % y a-set] [x y]))


(defn join-xf
  "Joins two relations (represented by zsets)
  based on predicates pred-1 and pred-2 and index key functions index-k-1 and index-k-2.
  index-state is an atom containing a map of index UUIDs (one for each of the two relations) to indexed zsets."
  [pred-1 index-k-1 pred-2 index-k-2 index-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}}]
  (let [index-uuid-1 (random-uuid)
        index-uuid-2 (random-uuid)]
    (timbre/spy index-uuid-1)
    (timbre/spy index-uuid-2)
    (comp
      ;receives a zset
      (mapcat identity)
      ;receives a zset item (with zset weight)
      (map (fn [zset-item]
             (let [delta-1 (if (pred-1 zset-item) (zs/index (timbre/spy #{zset-item}) index-k-1) {})
                   delta-2 (if (pred-2 zset-item) (zs/index #{zset-item} index-k-2) {})
                   zset    (if last? #{} #{zset-item})]
               ;return
               [delta-1 delta-2 zset])))
      (pxf/cond-branch
        ;does this xf care about the current item?
        (fn [[delta-1 delta-2 _zset :as delta-1+delta-2+zset]]
          (timbre/spy delta-1+delta-2+zset)
          ;if none of the predicates were true...
          (and (empty? delta-1) (empty? delta-2)))
        (map (fn [[_delta-1 _delta-2 zset]]
               (timbre/info "passing through...")
               (timbre/spy last?)
               (timbre/spy zset)))
        ;else, proceed to join
        any?
        (comp
          (map (fn [[delta-1 delta-2 zset]]
                 (let [index-state-1-prev (get @index-state index-uuid-1 {})
                       index-state-2-prev (get @index-state index-uuid-2 {})]
                   ;advance indices
                   (swap! index-state
                     (fn [state]
                       (-> state
                         (update index-uuid-1 (fn [index] (timbre/spy (zs/indexed-zset-pos+ index delta-1))))
                         (update index-uuid-2 (fn [index] (timbre/spy (zs/indexed-zset-pos+ index delta-2)))))))
                   ;return
                   [index-state-1-prev index-state-2-prev [delta-1 delta-2 zset]])))
          (map (fn [[index-state-1-prev index-state-2-prev [delta-1 delta-2 zset]]]
                 (timbre/spy zset)
                 (timbre/spy [delta-1 index-state-2-prev])
                 (timbre/spy [index-state-1-prev delta-2])
                 ;return
                 (vector
                   (zs/indexed-zset->zset
                     (zs/indexed-zset+
                       ;ΔTeam ⋈ Players
                       (timbre/spy (zs/join-indexed* delta-1 index-state-2-prev))
                       ;Teams ⋈ ΔPlayers
                       (timbre/spy (zs/join-indexed* index-state-1-prev delta-2))
                       ;ΔTeams ⋈ ΔPlayers
                       (timbre/spy (zs/join-indexed* delta-1 delta-2)))
                     return-zset-item-xf)
                   zset)))
          (mapcat (fn [[join-xf-delta zset]]
                    [(timbre/spy join-xf-delta) zset])))))))

(defn group-by-count-xf
  "Takes a group-by-style function f and returns a transducer which

  1. indexes the zset by f
  2. counts the number of items in each group
  3. returns an indexed zset (a map) where the keys are the result of f and the values are the counts

  Counts are represented as special zsets containing only one item with the count as the weight"
  [f]
  (comp
    (map (fn [zset] (zs/index zset f)))
    (map (fn [indexed-zset]
           (update-vals indexed-zset
             (fn [indexed-zset-item]
               (zs/zset-count
                 (transduce (map zs/zset-weight) + indexed-zset-item))))))))


(defn join-right-pred-1-xf
  "Joins already joined relations with a new relation.
  Modifies pred-1 and index-k-1 to point to the joined relations' second relation."
  [pred-1 index-k-1 pred-2 index-k-2 index-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}
      :as   params-map}]
  (join-xf (comp pred-1 second) (comp index-k-1 second) pred-2 index-k-2 index-state params-map))


(defn join-left-pred-1-xf
  "Joins already joined relations with a new relation.
  Modifies pred-1 and index-k-1 to point to the joined relations' first relation."
  [pred-1 index-k-1 pred-2 index-k-2 index-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}
      :as   params-map}]
  (join-xf (comp pred-1 first) (comp index-k-1 first) pred-2 index-k-2 index-state params-map))


(defn join-right-pred-2-xf
  "Joins already joined relations with a new relation.
  Modifies pred-2 and index-k-2 to point to the joined relations' second relation."
  [pred-1 index-k-1 pred-2 index-k-2 index-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}
      :as   params-map}]
  (join-xf pred-1 index-k-1 (comp pred-2 second) (comp index-k-2 second) index-state params-map))


(defn join-left-pred-2-xf
  "Joins already joined relations with a new relation.
  Modifies pred-2 and index-k-2 to point to the joined relations' first relation."
  [pred-1 index-k-1 pred-2 index-k-2 index-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}
      :as   params-map}]
  (join-xf pred-1 index-k-1 (comp pred-2 first) (comp index-k-2 first) index-state params-map))


(defn with-meta-f
  "Takes a function f and returns a function which takes data and returns (f data) with the same meta"
  [f]
  (fn [data]
    (with-meta
      (f data)
      (meta data))))


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

(defn- init-result [result result-delta]
  (if (nil? result)
    ;init
    (cond
      (map? result-delta) [{} zs/indexed-zset-pos+]
      (set? result-delta) [#{} zs/zset-pos+]
      :else (throw (ex-info "result-delta must be either map or set"
                     {:result-delta result})))
    ;else, existing result
    (cond
      (and (map? result) (map? result-delta)) [result zs/indexed-zset-pos+]
      (and (set? result) (set? result-delta)) [result zs/zset-pos+]
      :else (throw (ex-info "result and result-delta together must be either maps or sets"
                     {:result result :result-delta result})))))

(defn query-result-state-xf
  "Saves query results to atom. Works for both zsets and indexed-zset results."
  [result-state]
  (map (fn [result-delta]
         (timbre/spy result-delta)
         (swap! result-state
           (fn [result]
             (let [[result result+] (init-result result result-delta)]
               (result+ result result-delta)))))))


(defn disj-irrelevant-items [zset & preds]
  (transduce
    (filter (apply some-fn preds))
    conj
    #{}
    zset))
