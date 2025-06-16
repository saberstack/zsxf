(ns org.zsxf.input.datascript
  (:require [org.zsxf.query :as q]
            [org.zsxf.datom :as d2]
            [datascript.core :as d]
            [org.zsxf.util :as util]
            [taoensso.timbre :as timbre]))

(defn listen!
  "Initialize a listener for a given query and connection.
  Supports a time-f function that will be called with the time taken to process each transaction.
  Returns true"
  [conn query & {:keys [tx-time-f] :or {tx-time-f identity}}]
  (d/listen! conn (q/get-id query)
    (fn [tx-report]
      (util/time-f
        (q/input query (d2/ds-tx-datoms->datoms2->zsets (:tx-data tx-report)))
        tx-time-f)))
  ;return
  true)

(defn init-query-with-conn
  "Initial naive implementation.
  Assumes no writes are incoming during initialization.
  WIP

   Args:
    - query: a ZSXF query created with (q/create-query ...)
    - conn: a Datascript connection

   Opts
  `:listen?` defaults to true
    false can be used for testing/REPL purposes,
    or whenever we need the result of a query just once without continuous updates."
  [query conn & {:keys [listen? tx-time-f] :or {listen? true tx-time-f identity}}]
  (let [db      @conn
        ;load all existing data from a stable db state
        _result (q/input query
                  (d2/ds-tx-datoms->datoms2->zsets
                    (d/seek-datoms db :eavt)))]
    ;setup listener
    (when listen?
      (listen! conn query :tx-time-f tx-time-f))
    ;return
    true))

(defn get-result
  "Experimental proof-of-concept for parameterized queries"
  [conn query param]
  (d/transact! conn [{:zsxf.input/id param}])
  (let [return (q/get-result query)]
    (d/transact! conn [[:db/retractEntity [:zsxf.input/id param]]])
    return))

(comment
  ;WIP parameterized query example
  (let [query-all (q/create-query
                    (static-compile
                      '[:find ?d
                        :where
                        [?e :cell/data ?d]
                        [?e :cell/id ?cell-id]
                        [?p :zsxf.input/id ?cell-id]]))]
    (def query-all query-all)
    (init-query-with-conn query-all @*conn))

  (q/get-result query-all)

  (time
    (get-result @*conn query-all (rand-int 1000))))

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
  "Unlisten all Datascript listeners.
  Args:
    - conn: a Datascript connection
  "
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
