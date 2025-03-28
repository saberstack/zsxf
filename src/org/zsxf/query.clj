(ns org.zsxf.query
  (:require [org.zsxf.xf :as xf]))


(defn create-query
  "Create a query with init-xf and optional state atom.
  If state is not provided, one will be created.
  init-xf is a function which takes an atom and must return a ZSXF-compatible transducer which takes and returns zsets
  Returns a map."
  ([init-xf]
   (create-query init-xf (atom nil)))
  ([init-xf state]
   {:xf (init-xf state)
    :state state}))


(defn input
  "Takes a query (a map created via create-query) and a vector of zsets representing a transaction.
  Synchronously executes the transaction and returns the resulting query state."
  [{:keys [state xf] :as _query} zsets]
  (transduce
    xf
    (fn
      ;query reducing fn
      ; sums existing result with query-computed deltas
      ([] state)
      ([accum] accum)
      ([state result-delta]
       ;side effect
       (swap! state
         (fn [{:keys [result] :as state-m}]
           (let [[result result+] (xf/init-result result result-delta)]
             (assoc state-m :result
               (result+ result result-delta)))))))
    [zsets]))

(defn get-result
  "View the current query result"
  [query]
  (:result @(get query :state)))

(defn get-state [query]
  @(get query :state))
