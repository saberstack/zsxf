(ns org.zsxf.query
  (:require [org.zsxf.constant :as const]
            [org.zsxf.util :as util]
            [org.zsxf.zset :as zs]
            [org.zsxf.query :as-alias q]
            [taoensso.timbre :as timbre]))

(defn create-query
  "Create a query with init-xf.
  init-xf is a function which takes a single argument,
  and must return a ZSXF-compatible transducer which takes and returns zsets
  Returns a map."
  [init-xf]
  (let [state (atom nil)]
    {::q/xf             (init-xf state)
     ::q/state          state
     ;TODO save tx->t for history
     ;potential data structure:
     #_{:t->vector-idx {42 0 43 1}
        :history       [#_result-state-0 #_result-state-1]}
     ::q/result-history (atom [])
     ::q/id             (random-uuid)
     ::q/keep-history?  true}))

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

  (timbre/info "basis-t:" basis-t)
  (transduce
    xf
    (fn
      ([] state)                                            ;init
      ([state result-delta]                                 ;reduce step
       ;query reducing fn; sums the existing result with query-computed deltas
       ;side effects
       (when keep-history?
         (swap! result-history conj (::q/result @state)))
       ;new query state
       (swap! state
         (fn [{::q/keys [result] :as state-m}]
           (let [[result+ result] (init-result result result-delta)]
             (assoc state-m ::q/result
               (result+ result result-delta))))))
      ([state-m]                                            ;finalize
       (::q/result state-m)))
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
