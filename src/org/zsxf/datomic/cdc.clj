(ns org.zsxf.datomic.cdc
  (:require
   [datomic.api :as dd]
   [net.cgrand.xforms :as xforms]
   [ss.loop]
   [clojure.core.async :as a]
   [taoensso.timbre :as timbre]))

(defn db-uri [db-name]
  (str "datomic:sql://" db-name "?jdbc:sqlite:./datomic/storage/sqlite.db"))

(defn get-all-idents [conn]
  (dd/q
    '[:find ?e ?attr
      :where
      [?e :db/ident ?attr]]
    (dd/db conn)))

(defn tx-data->datoms
  [idents-m data]
  (into []
    (map (fn [[e a v t tf :as datom]]
           (let [a' (get idents-m a)]
             [e a' v t tf])))
    data))

;Incremental CDC:
; 1. Record last :t value
; 2. Call get-log-transactions with (inc t)
; 3. Repeat

(defonce cdc-ch (atom (a/chan 42)))
(defonce cdc-last-t (atom nil))

(defn ->reduce-to-chan [ch]
  (fn
    ([] ch)
    ([accum-ch] accum-ch)
    ([accum-ch item]
     (timbre/info "reducing" item)
     (a/>!! accum-ch item)
     accum-ch)))

(defn datomic-tx-log->output
  "Retrieves transactions from the Datomic log and processes them via a transducer.
   Args:
     conn - Datomic connection
     reducing-f - A reducing function that processes the transformed transactions
     ->xf - Function that takes an idents map and returns a transducer for transforming transactions
     start - Optional start time/transaction ID to retrieve logs from (default nil)
     end - Optional end time/transaction ID to retrieve logs until (default nil)
  "
  ([conn output-reducing-f] (datomic-tx-log->output conn output-reducing-f (fn [_idents-m] (map identity)) nil nil))
  ([conn output-reducing-f ->xf] (datomic-tx-log->output conn output-reducing-f ->xf nil nil))
  ([conn output-reducing-f ->xf start end]
   (let [log          (dd/log conn)
         idents-m     (into {} (get-all-idents conn))
         transactions (dd/tx-range log start end)]
     (transduce
       (comp
         (map (fn [tx-m]
                ; Store the last seen transaction ID :t
                (reset! cdc-last-t (:t tx-m))
                ;return tx-m unchanged
                tx-m))
         (->xf idents-m))
       output-reducing-f
       transactions))))

(defn react-on-transaction!
  [conn on-transaction]
  ;TODO WIP
  (let [tx-queue (dd/tx-report-queue conn)]
    (a/thread
      (while true
        (let [txn (.take tx-queue)]
          (try
            (when (:tx-data txn)
              (on-transaction txn))
            (catch Exception e (timbre/error e "on-transaction error"))))))))

(defn start-cdc!
  []
  (reset! cdc-ch (a/chan 10))
  #_(ss.loop/go-loop ^{:id :datomic-cdc-loop}
      [i 0]
      (timbre/info "looping..." i)
      (a/<! (a/timeout 1000))
      (recur (inc i))))

(comment

  (let [conn (dd/connect (db-uri "mbrainz"))]
    (react-on-transaction! conn (fn [txn] (timbre/info "txn:" txn))))


  (dd/create-database (db-uri "zsxf"))

  (dd/get-database-names (db-uri "*"))

  (dd/connect (db-uri "zsxf"))

  (def conn (dd/connect (db-uri "zsxf")))


  (get-all-idents (dd/connect (db-uri "mbrainz")))

  ;return all datoms in the db (including internal setup datoms)
  (into []
    (dd/seek-datoms (dd/db (dd/connect (db-uri "zsxf"))) :eavt))

  (let [conn (dd/connect (db-uri "zsxf"))]
    (dd/transact conn [{:db/ident       :movie/title
                        :db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/doc         "The title of the movie"}]))

  (let [conn (dd/connect (db-uri "zsxf"))]
    (dd/t))

  (let [conn (dd/connect (db-uri "zsxf"))]
    (dd/q
      '[:find ?e ?v
        :where [?e :movie/title ?v]]
      (dd/db conn)))

  (dd/delete-database (db-uri "zsxf")))
