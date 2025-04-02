(ns org.zsxf.query-state
  (:require [org.zsxf.query :as-alias q]))

(defn query-state-f
  "Helper to modify query state without bothering the caller
  with the internal structure of the query atom."
  [f]
  (fn [query]
    (update query ::q/state f)))

(defn query-state
  [query]
  (::q/state query))
