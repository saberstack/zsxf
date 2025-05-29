(ns org.zsxf.datomic.core
  (:require
   [datascript.core :as d]
   [datomic.api :as dd]
   [clojure.core.async :as a]
   [org.zsxf.datom :as d2]
   [taoensso.timbre :as timbre]))

(defn db-uri [db-name]
  (str "datomic:sql://" db-name "?jdbc:sqlite:./datomic/storage/sqlite.db"))

(defn get-all-idents [conn]
  (dd/q
    '[:find ?e ?attr
      :where
      [?e :db/ident ?attr]]
    (dd/db conn)))

(defn test-tx []
  (let [conn (dd/connect (db-uri "zsxf"))]
    (dd/transact conn
      [{:movie/title "The Matrix"}])))

(defn test-q []
  (let [conn (dd/connect (db-uri "zsxf"))]
    (dd/q
      '[:find ?e ?v
        :where [?e :movie/title ?v]]
      (dd/db conn))))

;TODO Datomic datom attributes are not inline (different from Datascript)
;  - potentially get keyword from schema

(defonce run-datomic-cdc? (atom true))


(defn tx-data->datom-vs [idents-m data]
  (mapv
    (fn [[e a v t tf :as datom]]
      (let [a' (get idents-m a)]
        ;datom
        [e a' v t tf]))
    data))

;Incremental CDC:
; 1. Record last :t value
; 2. Call get-log-transactions with (inc t)
; 3. Repeat

(defn get-log-transactions
  "- `conn`: The Datomic connection."
  [conn]
  (let [log          (dd/log conn)
        idents-m     (into {} (get-all-idents conn))
        transactions (dd/tx-range log 2553037 nil)]
    ;TODO WIP
    (sequence
      (mapcat
        (fn [{:keys [data t]}]
          (tx-data->datom-vs idents-m data)))
      transactions)))

(defn react-on-transaction! [conn on-transaction]
  (let [tx-queue (dd/tx-report-queue conn)]
    (a/thread
      (while true
        (let [txn (.take tx-queue)]
          (try
            (when (:tx-data txn)
              (on-transaction txn))
            (catch Exception e (timbre/error e "on-transaction error"))))))))

(defn init-query-with-conn
  "Initial naive implementation. No listeners or change data capture.
  Read all transactions datoms."
  [query conn]
  (get-log-transactions conn))

(comment

  (let [conn (dd/connect (db-uri "mbrainz"))]
    (take 2 (get-log-transactions conn)))

  (let [conn (dd/connect (db-uri "mbrainz"))]
    (react-on-transaction! conn (fn [txn] (timbre/info "txn:" txn))))


  (dd/create-database (db-uri "zsxf"))

  (dd/get-database-names (db-uri "*"))

  (dd/connect (db-uri "zsxf"))

  (get-log-transactions conn)


  (def conn (dd/connect (db-uri "zsxf")))


  (get-all-idents (dd/connect (db-uri "zsxf")))

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
