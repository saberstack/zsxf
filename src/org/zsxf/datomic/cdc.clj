(ns org.zsxf.datomic.cdc
  (:require
   [datomic.api :as dd]
   [ss.loop]
   [org.zsxf.query :as-alias q]
   [clojure.core.async :as a]
   [taoensso.timbre :as timbre]
   [org.zsxf.datomic.cdc :as-alias dcdc])
  (:import (datomic Connection)))

(defn conn? [x]
  (instance? Connection x))

(defn uri-sqlite
  ([db-name] (uri-sqlite db-name "./datomic/storage/sqlite.db"))
  ([db-name path]
   (str "datomic:sql://" db-name "?jdbc:sqlite:" path)))

(defn get-all-idents [conn]
  (dd/q
    '[:find ?e ?attr
      :where
      [?e :db/ident ?attr]]
    (dd/db conn)))

(defn all-basis-t
  "Returns all basis-t values in the database.
  Depending on the total number of transactions,
  this might return a very large vector."
  ([conn] (all-basis-t conn nil nil))
  ([conn start end]
   (eduction
     (map (fn [tx-m] (:t tx-m)))
     (dd/tx-range (dd/log conn) start end))))

(defn log->output
  "Retrieves transactions from the Datomic log and processes them via a transducer.
   Args:
     state - atom that holds the query state
     conn - Datomic connection
     output-rf - A reducing function that processes the transformed transactions
     xform - Function that takes an idents map and returns a transducer for transforming transactions
     start - Optional start time/transaction ID to retrieve logs from (default nil)
     end - Optional end time/transaction ID to retrieve logs until (default nil)
  "
  ([query-state conn xform output-rf]
   ; Call with no start or end, which means process all transactions
   (log->output query-state conn xform output-rf nil nil))
  ([query-state conn xform output-rf start end]
   (let [idents-m     (into {} (get-all-idents conn))
         transactions (dd/tx-range (dd/log conn) start end)]
     (transduce
       (comp
         (map (fn [tx-m]
                ; Store the last seen transaction ID :t
                (swap! query-state assoc ::dcdc/last-t-processed (:t tx-m))
                ;return tx-m unchanged
                tx-m))
         (xform idents-m))
       output-rf
       ;for side effects, no init
       nil
       transactions))))

(defn on-transaction-loop!
  "Function to reactively sync after a transaction"
  [id conn]
  (let [tx-report-queue-ch (a/chan (a/sliding-buffer 1))
        tx-queue           (dd/tx-report-queue conn)]
    ;TODO WIP, not implemented
    #_(ss.loop/go-loop
        ^{:id [id :react-on-transaction]}
        []
        (let [t          (a/<! (a/thread (let [tx (.take tx-queue)]
                                           (try
                                             (when (:tx-data tx) (dd/basis-t (:db-after tx)))
                                             (catch Exception e (timbre/error e "on-transaction error"))))))
              put-return (a/put! tx-report-queue-ch t)]
          (timbre/info "put-return" put-return)
          (timbre/info "reacting on new t:" t)
          (recur)))
    ;return channel
    tx-report-queue-ch))

(defn start-log->output!
  "Starts a loop that continuously processes Datomic transaction logs.

   Creates a go-loop that processes transactions, waits for new ones (or timeout),
   and updates the processing window based on completed transactions.

   Args:
     id - Unique identifier for the loop
     conn - Datomic connection
     output-rf - Reducing function that processes transformed transactions
     xform - Function that takes an idents map and returns a transducer for transforming transactions

   Use stop-all-loops! to stop."
  [{::q/keys [state id] :as _query} conn xform output-rf]
  (let [tx-report-queue-ch (on-transaction-loop! id conn)]
    (ss.loop/go-loop
      ^{:id [id :log->output]}
      [start nil
       end nil]
      (log->output state conn xform output-rf start end)
      (let [timeout-ch       (a/timeout 5000)
            [last-t-on-report-queue _ch] (a/alts! [timeout-ch tx-report-queue-ch])
            _                (timbre/info "last-t-on-report-queue" last-t-on-report-queue)
            last-t-processed (get @state ::dcdc/last-t-processed)
            next-start       (when (int? last-t-processed) (inc last-t-processed))
            next-end         (when (int? last-t-on-report-queue) (inc last-t-on-report-queue))]
        (timbre/info "log->output-loop! [next-start next-end]" [next-start next-end])
        (recur next-start next-end)))))

(defn stop-all-loops! [id]
  (ss.loop/stop [id :log->output])
  (ss.loop/stop [id :react-on-transaction]))

(comment


  (dd/create-database (uri-sqlite "zsxf"))

  (dd/get-database-names (uri-sqlite "*"))

  (dd/connect (uri-sqlite "zsxf"))

  (def conn (dd/connect (uri-sqlite "zsxf")))


  (get-all-idents (dd/connect (uri-sqlite "mbrainz")))

  ;return all datoms in the db (including internal setup datoms)
  (into []
    (dd/seek-datoms (dd/db (dd/connect (uri-sqlite "zsxf"))) :eavt))

  (let [conn (dd/connect (uri-sqlite "zsxf"))]
    (dd/transact conn [{:db/ident       :movie/title
                        :db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/doc         "The title of the movie"}]))

  (let [conn (dd/connect (uri-sqlite "zsxf"))]
    (dd/t))

  (let [conn (dd/connect (uri-sqlite "zsxf"))]
    (dd/q
      '[:find ?e ?v
        :where [?e :movie/title ?v]]
      (dd/db conn)))

  (dd/delete-database (uri-sqlite "zsxf")))
