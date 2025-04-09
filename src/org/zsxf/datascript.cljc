(ns org.zsxf.datascript
  (:require [org.zsxf.query :as q]
            [org.zsxf.datom2 :as d2]
            [datascript.core :as d]
            [taoensso.timbre :as timbre]))

(defn listen!
  "Initialize a listener for a given query and connection.
  Supports a time-f function that will be called with the time taken to process each transaction.
  Returns true"
  [conn query]
  (d/listen! conn (q/get-id query)
    (fn [tx-report]
      (q/input query (timbre/spy (d2/tx-datoms->datoms2->zsets (:tx-data tx-report))))))
  ;return
  true)

(defn init-query-with-conn
  "Initial naive implementation.
  Assumes no writes are incoming during initialization.
  WIP"
  [query conn & {:keys [time-f] :or {time-f identity}}]
  (let [db      @conn
        ;load all existing data from a stable db state
        _result (q/input query
                  (d2/tx-datoms->datoms2->zsets
                    (d/seek-datoms db :eavt)))]
    ;setup listener
    (listen! conn query)))

(defn take-last-datoms
  "Helper to see recently added datoms"
  [conn n]
  (into [] (take n) (d/rseek-datoms @conn :eavt)))

(defn conn-listeners
  "Helper that returns current Datascript conn listeners.
  Returns a map of listener key -> fn (which receives tx-report)
  Warning: This is likely a Datascript implementation detail.
  It could break without warning."
  [conn]
  (:listeners @(:atom conn)))

(defn unlisten-all!
  "Unlisten all Datascript listeners."
  [conn]
  (run! (fn [[k _]] (d/unlisten! conn k)) (conn-listeners conn)))

;examples
(comment
  (set! *print-meta* true)
  (=
    (with-meta
      (d2/datom2 (d/datom 1 :a "v"))
      {:mmmm 42})
    (with-meta
      (d2/datom2 (d/datom 1 :a "v"))
      {:mmmm 43})))
