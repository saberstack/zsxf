(ns org.zsxf.jdbc.postgres
  (:require
   [config.core :as config]
   [honey.sql :as hsql]
   [next.jdbc :as jdbc]
   [next.jdbc.connection :as connection]
   [next.jdbc.result-set :as rs]
   [next.jdbc.prepare :as prepare]
   [taoensso.timbre :as timbre])
  (:import (com.zaxxer.hikari HikariDataSource)))

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

(defn init-db-connection-pool []
  (when (nil? @*db-conn-pool)
    (let [{:keys [dbtype host dbname user password]} (jdbc-postgres-params)]
      (reset! *db-conn-pool
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
  []
  (timbre/spy
    (jdbc/execute! @*db-conn-pool ["SELECT 1;"])))

(defn init
  "Initializes the database connection pool and checks the connection."
  []
  (init-db-connection-pool)
  (check-db-connection-pool!))


(defn table->zsets [db-conn fully-qualified-table-name]
  )
