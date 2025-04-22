(ns org.zsxf.query
  (:require [org.zsxf.constant :as const]
            [org.zsxf.zset :as zs]
            [org.zsxf.query :as-alias q]))

(defn create-query
  "Create a query with init-xf.
  init-xf is a function which takes a single argument,
  and must return a ZSXF-compatible transducer which takes and returns zsets
  Returns a map."
  ([init-xf]
   (create-query init-xf []))
  ([init-xf custom-init]
   (let [state (atom nil)]
     {::q/xf          (init-xf state)
      ::q/state       state
      ::q/id          (random-uuid)
      ::q/custom-init custom-init})))

(defn init-result [result result-delta [reduce-fn reduce-init]]
  (if (nil? result)
    ;init
    (cond
      (and (fn? reduce-fn) (coll? reduce-init)) [reduce-fn reduce-init]
      (map? result-delta) [zs/indexed-zset+ {}]             ;for aggregates, allow negative weights
      (set? result-delta) [zs/zset-pos+ #{}]                ;regular joins, no negative weight
      :else (throw (ex-info "result-delta must be either map or set"
                     {:result-delta result})))
    ;else, existing result
    (cond
      (and (fn? reduce-fn) (coll? reduce-init)) [reduce-fn result]
      (and (map? result) (map? result-delta)) [zs/indexed-zset+ result] ;for aggregates, allow negative weights
      (and (set? result) (set? result-delta)) [zs/zset-pos+ result] ;regular joins no negative weights
      :else (throw (ex-info "result and result-delta together must be either maps or sets"
                     {:result result :result-delta result})))))

(defn input
  "Takes a query (a map created via create-query) and a vector of zsets representing a transaction.
  Synchronously executes the transaction, summing the existing query result state with new deltas, if any.
  Returns the full post-transaction query result.
  The result can be a set or a map (in the case of aggregations)."
  [{::q/keys [state xf custom-init] :as _query} zsets]
  (transduce
    xf
    (fn
      ;query reducing fn
      ; sums existing result with query-computed deltas
      ([] state)
      ([state] (get state ::q/result))
      ([state result-delta]
       ;side effect
       (swap! state
         (fn [{::q/keys [result] :as state-m}]
           (let [[result+ result] (init-result result result-delta custom-init)]
             (assoc state-m ::q/result
               (result+ result result-delta)))))))
    [zsets]))

(defn get-result
  "View the current query result"
  [query]
  ;(set! *print-meta* true)
  (::q/result @(get query ::q/state)))

(defn get-aggregate-result
  "WIP fn"
  [query]
  (update-vals
    (get-result query)
    (fn [s]
      (into #{}
        (comp
          (map (fn [x] (if (= x const/zset-sum) [:sum (zs/zset-weight x)] x)))
          (map (fn [x] (if (= x const/zset-count) [:count (zs/zset-weight x)] x))))
        s))))

(defn get-state [query]
  @(get query ::q/state))

(defn get-id
  "Returns the unique query identifier (default is UUID)"
  [query]
  (get query ::q/id))
