(ns org.zsxf.datomic.cdc
  (:require
   [datomic.api :as dd]
   [ss.loop]
   [clojure.core.async :as a]
   [taoensso.timbre :as timbre])
  (:import (datomic Connection)))

(defn conn? [x]
  (instance? Connection x))

(defn db-uri-sqlite
  ([db-name] (db-uri-sqlite db-name "./datomic/storage/sqlite.db"))
  ([db-name path]
   (str "datomic:sql://" db-name "?jdbc:sqlite:" path)))

(defn get-all-idents [conn]
  (dd/q
    '[:find ?e ?attr
      :where
      [?e :db/ident ?attr]]
    (dd/db conn)))

(defn ->reduce-to-chan [ch]
  (fn
    ([] ch)
    ([accum-ch] accum-ch)
    ([accum-ch item]
     (a/>!! accum-ch item)
     accum-ch)))

(defn log->output
  "Retrieves transactions from the Datomic log and processes them via a transducer.
   Args:
     cdc-state - atom that holds CDC state
     conn - Datomic connection
     output-rf - A reducing function that processes the transformed transactions
     ->xf - Function that takes an idents map and returns a transducer for transforming transactions
     start - Optional start time/transaction ID to retrieve logs from (default nil)
     end - Optional end time/transaction ID to retrieve logs until (default nil)
  "
  ([cdc-state conn output-rf]
   (log->output cdc-state conn output-rf (fn [_idents-m] (map identity)) nil nil))
  ([cdc-state conn output-rf ->xf]
   (log->output cdc-state conn output-rf ->xf nil nil))
  ([cdc-state conn output-rf ->xf start end]
   (let [log          (dd/log conn)
         idents-m     (into {} (get-all-idents conn))
         transactions (dd/tx-range log start end)]
     (transduce
       (comp
         (map (fn [tx-m]
                ; Store the last seen transaction ID :t
                (swap! cdc-state assoc :last-t-processed (:t tx-m))
                ;return tx-m unchanged
                tx-m))
         (->xf idents-m))
       output-rf
       transactions))))

(defn on-πtransaction-loop!
  "Function to reactively sync after a transaction"
  [id conn]
  (let [tx-report-queue-ch (a/chan (a/sliding-buffer 1))
        tx-queue (dd/tx-report-queue conn)]
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

(defn log->output-loop!
  [id conn output-rf ->xf]
  (let [cdc-state          (atom {:last-t-processed nil})
        tx-report-queue-ch (on-transaction-loop! id conn)]
    (ss.loop/go-loop
      ^{:id [id :log->output]}
      [start nil
       end   nil]
      (log->output cdc-state conn output-rf ->xf start end)
      (let [timeout-ch       (a/timeout 5000)
            [last-t-on-report-queue _ch] (a/alts! [timeout-ch tx-report-queue-ch])
            _                (timbre/info "last-t-on-report-queue" last-t-on-report-queue)
            last-t-processed (get @cdc-state :last-t-processed)
            next-start       (when (int? last-t-processed) (inc last-t-processed))
            next-end         (when (int? last-t-on-report-queue) (inc last-t-on-report-queue))]
        (timbre/info "log->output-loop! [next-start next-end]" [next-start next-end])
        (recur next-start next-end)))))

(defn stop-all-loops! [id]
  (ss.loop/stop [id :log->output])
  (ss.loop/stop [id :react-on-transaction]))

(comment


  (dd/create-database (db-uri-sqlite "zsxf"))

  (dd/get-database-names (db-uri-sqlite "*"))

  (dd/connect (db-uri-sqlite "zsxf"))

  (def conn (dd/connect (db-uri-sqlite "zsxf")))


  (get-all-idents (dd/connect (db-uri-sqlite "mbrainz")))

  ;return all datoms in the db (including internal setup datoms)
  (into []
    (dd/seek-datoms (dd/db (dd/connect (db-uri-sqlite "zsxf"))) :eavt))

  (let [conn (dd/connect (db-uri-sqlite "zsxf"))]
    (dd/transact conn [{:db/ident       :movie/title
                        :db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/doc         "The title of the movie"}]))

  (let [conn (dd/connect (db-uri-sqlite "zsxf"))]
    (dd/t))

  (let [conn (dd/connect (db-uri-sqlite "zsxf"))]
    (dd/q
      '[:find ?e ?v
        :where [?e :movie/title ?v]]
      (dd/db conn)))

  (dd/delete-database (db-uri-sqlite "zsxf")))
