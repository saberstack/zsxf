(ns org.zsxf.query
  (:require [org.zsxf.xf :as xf]
            [org.zsxf.query :as q]))


#_(defn create-query
  "Create a query with init-xf and optional state atom.
  If state is not provided, one will be created.
  init-xf is a function which takes an atom and must return a ZSXF-compatible transducer which takes and returns zsets
  Returns a map."
  ([init-xf]
   (create-query init-xf (atom nil)))
  ([init-xf state]
   {::q/xf    (init-xf state)
    ::q/state state
    ::q/id    (random-uuid)}))

(defn create-query
  "Create a query with init-xf.
  init-xf is a function which takes an atom and must return a ZSXF-compatible transducer which takes and returns zsets
  Returns an atom"
  [init-xf]
  (let [query (atom nil)]
    (reset! query
      {::q/xf    (init-xf query)
       ::q/state nil
       ::q/id    (random-uuid)})
    query))


#_(defn input
  "Takes a query (a map created via create-query) and a vector of zsets representing a transaction.
  Synchronously executes the transaction, summing the existing query result state with new deltas, if any.
  Returns the full post-transaction query result.
  The result can be a set or a map (in the case of aggregations)."
  [{::q/keys [state xf] :as _query} zsets]
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
           (let [[result result+] (xf/init-result result result-delta)]
             (assoc state-m ::q/result
               (result+ result result-delta)))))))
    [zsets]))

(defn input
  "Takes a query (a map created via create-query) and a vector of zsets representing a transaction.
  Synchronously executes the transaction, summing the existing query result state with new deltas, if any.
  Returns the full post-transaction query result.
  The result can be a set or a map (in the case of aggregations)."
  [query zsets]
  (transduce
    (::q/xf @query)
    (fn
      ;query reducing fn
      ; sums existing result with query-computed deltas
      ([] query)
      ([query-m-final]
       ;return from transduce
       (-> query-m-final ::q/state ::q/result))
      ([query result-delta]
       ;side effect
       (swap! query
         (fn [{{::q/keys [result]} ::q/state :as query-m}]
           (let [[result result+] (xf/init-result result result-delta)]
             (assoc-in query-m [::q/state ::q/result]
               (result+ result result-delta)))))
       ;returns query-m-final
       ))
    [zsets]))

#_(defn get-result
  "View the current query result"
  [query]
  (set! *print-meta* true)
  (::q/result @(get query ::q/state)))

(defn get-result
  "View the current query result"
  [query]
  (set! *print-meta* true)
  (-> @query ::q/state ::q/result))

#_(defn get-state [query]
  @(get query ::q/state))

(defn get-state [query]
  (::q/state @query))

#_(defn get-id
  "Returns the unique query identifier (default is UUID)"
  [query]
  (get query ::q/id))

(defn get-id
  "Returns the unique query identifier (default is UUID)"
  [query]
  (::q/id @query))
