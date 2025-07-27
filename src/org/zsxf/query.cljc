(ns org.zsxf.query
  (:require [org.zsxf.constant :as const]
            [org.zsxf.util :as util]
            [org.zsxf.zset :as zs]
            [org.zsxf.query :as-alias q]))

(defn create-query
  "Create a query with init-xf.
  init-xf is a function which takes a single argument,
  and must return a ZSXF-compatible transducer which takes and returns zsets
  Returns a map."
  [init-xf & {:keys [keep-history?] :or {keep-history? false}}]
  (let [state (atom nil)]
    {::q/xf             (init-xf state)
     ::q/state          state
     ::q/result-history (atom {})
     ::q/id             (random-uuid)
     ::q/keep-history?  keep-history?}))

(defn init-result [result result-delta]
  (if (nil? result)
    ;init
    (cond
      (map? result-delta) [zs/indexed-zset+ {}]             ;for aggregates, allow negative weights
      (set? result-delta) [zs/zset-pos+ #{}]                ;regular joins, no negative weight
      :else (throw (ex-info "result-delta must be either map or set"
                     {:result-delta result})))
    ;else, existing result
    (cond
      (and (map? result) (map? result-delta)) [zs/indexed-zset+ result] ;for aggregates, allow negative weights
      (and (set? result) (set? result-delta)) [zs/zset-pos+ result] ;regular joins no negative weights
      :else (throw (ex-info "result and result-delta together must be either maps or sets"
                     {:result result :result-delta result})))))

(defn get-result
  "View the current query result"
  [query]
  ;(set! *print-meta* true)
  (::q/result @(::q/state query)))

(defn- history-append!
  "Implementation detail:
  Appends a new query result at the given basis-t to the query result history."
  [result-history basis-t query-result-at-basis]
  (swap! result-history
    (fn [m]
      (let [history'             (conj (:result-states m []) query-result-at-basis)
            vector-idx           (dec (count history'))
            basis-t->vector-idx' (assoc (:basis-t->vector-idx m {}) basis-t vector-idx)]
        (-> m
          (assoc :result-states history')
          (assoc :basis-t->vector-idx basis-t->vector-idx'))))))

(defn get-result-as-of
  "Retrieves the query result at a specific basis-t (point in time).

  This function provides time-travel capabilities by looking up the historical
  query result that was recorded at the given basis-t. It accesses the query's
  result history to find the corresponding result state.

  Parameters:
  - query: A query map created by create-query containing result history
  - basis-t: The original database/source basis time for which to retrieve the result

  Returns:
  - The query result that existed at the specified basis-t
  - nil if no result exists for the given basis-t

  Note: This function only works when the query has history tracking enabled, i.e.
  (::q/keep-history? query) is true"
  [query basis-t]
  (let [{:keys [result-states basis-t->vector-idx]} @(::q/result-history query)
        idx (get basis-t->vector-idx basis-t)]
    (when (int? idx)
      (result-states idx))))

(defn history-index
  "Implementation detail.
  Returns the map of basis-t to vector index for the query result history."
  [query]
  (let [{:keys [basis-t->vector-idx]} @(::q/result-history query)]
    basis-t->vector-idx))

(defn input
  "Takes a query (a map created via create-query) and a vector of zsets representing a transaction.
  Synchronously executes the transaction, summing the existing query result state with new deltas, if any.

  Parameters:
  - query: A query map created by create-query containing:
    - ::q/state: Atom holding the query state
    - ::q/xf: Transducer that processes zsets
  - zsets: Input zsets to be processed by the query

  - Optional::
  - :basis-t: basis-t of the transaction from the original source (used for history tracking, if enabled)

  Returns the full post-transaction query result.
  The result can be a set or a map (in the case of aggregations)."
  [{::q/keys [state result-history xf keep-history?] :as _query} zsets
   & {:keys [basis-t]}]
  (transduce
    xf
    (fn
      ([] state)                                            ;init
      ([state result-delta]                                 ;reduce step
       ;query reducing fn; sums the existing result with query-computed deltas
       ;new query state
       (swap! state
         (fn [{::q/keys [result] :as state-m}]
           (let [[result+ result] (init-result result result-delta)]
             (assoc state-m ::q/result
               (result+ result result-delta))))))
      ([state-m]                                            ;finalize
       (let [query-result (::q/result state-m)]
         ;side effects
         (when (and (true? keep-history?) (int? basis-t))
           (history-append! result-history basis-t query-result))
         ;return
         query-result
         )))
    [zsets]))

(defn get-aggregate-result
  [query]
  (util/map-filter-vals
    (get-result query)
    (fn [new-map-value] (not= #{} new-map-value))
    (fn [s]
      (into #{}
        (comp
          (map (fn [[tag item :as v]]
                 (if (= item const/zset-sum)
                   [tag (zs/zset-weight v)]
                   v)))
          (map (fn [[tag item :as v]]
                 (if (= item const/zset-count)
                   [tag (zs/zset-weight v)]
                   v))))
        s))))

(defn get-state [query]
  @(get query ::q/state))

(defn get-id
  "Returns the unique query identifier (default is UUID)"
  [query]
  (get query ::q/id))
