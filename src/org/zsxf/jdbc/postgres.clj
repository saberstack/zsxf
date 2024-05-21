(ns org.zsxf.jdbc.postgres
  (:require
   [clojure.core.async :as a]
   [config.core :as config]
   [honey.sql :as hsql]
   [next.jdbc :as jdbc]
   [next.jdbc.connection :as connection]
   [next.jdbc.result-set :as rs]
   [next.jdbc.prepare :as prepare]
   [org.zsxf.zset :as zs]
   [taoensso.timbre :as timbre])
  (:import (clojure.lang IReduceInit)
           (com.zaxxer.hikari HikariDataSource)))

(defonce *db-conn-pool (atom nil))

(defn- jdbc-postgres-params
  "Returns a map with the parameters needed to create a JDBC connection."
  []
  {:dbtype   "postgres"
   ;Default environment variable names as specified at
   ; https://www.postgresql.org/docs/15/libpq-envars.html
   :host     (:pghost config/env)                           ; PGHOST
   :dbname   (:pgdatabase config/env)                       ; PGDATABASE
   :user     (:pguser config/env)                           ; PGUSER
   :password (:pgpassword config/env)                       ; PGPASSWORD
   })

(defn init-db-connection-pool [*atom]
  (when (nil? @*atom)
    (let [{:keys [dbtype host dbname user password]} (jdbc-postgres-params)]
      (reset! *atom
        (connection/->pool
          HikariDataSource
          ;; HikariCP-specific keys (Postgres connection pool)
          ;; https://github.com/seancorfield/next-jdbc/blob/fd95a69b5c41354fda55a36f4c6d6d5f088b7384/doc/getting-started.md#connection-pooling
          {:dbtype               dbtype
           :host                 host
           :dbname               dbname
           :username             user
           :password             password
           :connectionInitSql    "COMMIT;"
           :dataSourceProperties {:socketTimeout 30}})))))

(defn check-db-connection-pool!
  "Performs a check on the connection pool by issuing a basic query.
  Used to verify and initialize the pool connection to allow the next SQL query to execute faster."
  [db-conn]
  (timbre/spy
    (jdbc/execute! db-conn ["SELECT 1;"])))

(defn init
  "Initializes the database connection pool and checks the connection."
  []
  (init-db-connection-pool *db-conn-pool)
  (check-db-connection-pool! @*db-conn-pool))


(defn table->reducible [db-conn fully-qualified-table-name]
  (jdbc/plan db-conn
    (hsql/format
      {:select [:*]
       :from   [fully-qualified-table-name]
       :where [:<= :id 100000]
       })))


(def table-row->zset-xf
  (map (fn [row] (zs/zset #{row})))) ; transform a table row into a zset

(defn reducible->chan
  "Take the rows from the reducible and put them onto a channel. Return the channel."
  [^IReduceInit reducible ch]
  (future
    (transduce
      (comp
        (map (fn [row] (a/>!! ch row)))
        ; halt when the receiving channel is full
        ; WARNING: core.async sliding-buffer and dropping-buffer will not halt
        ;(halt-when nil?)
        )
      conj
      []
      (eduction
        (map (fn [row] (into {} row)))
        reducible))
    (a/close! ch))
  ;return channel
  ch)

(defonce *all-teams (atom nil))
(defonce *all-players (atom nil))

(defn init-all-data []
  (init)
  (reset! *all-teams
    (a/<!!
      (a/reduce
        conj
        []
        (reducible->chan
          (table->reducible @*db-conn-pool :saberstack.zsxf.team)
          (a/chan 1 table-row->zset-xf)))))

  (reset! *all-players
    (a/<!!
      (a/reduce
        conj
        []
        (reducible->chan
          (table->reducible @*db-conn-pool :saberstack.zsxf.player)
          (a/chan 1 table-row->zset-xf)))))
  :done)

(comment
  ;TODO process all-teams and all-players through a join dataflow

  (a/<!!
    (a/reduce
      conj
      []
      (reducible->chan
        (table->reducible @*db-conn-pool :saberstack.zsxf.team)
        (a/chan 42 #_(a/dropping-buffer 10) (map (fn [row] row))))))

  (a/<!!
    (a/reduce
      conj
      []
      (reducible->chan
        (table->reducible @*db-conn-pool :saberstack.zsxf.player)
        (a/chan 42 #_(a/dropping-buffer 10) (map (fn [row] row)))))))
