(ns org.zsxf.archive.jdbc.postgres
  (:require
   [clojure.core.async :as a]
   [config.core :as config]
   [honey.sql :as hsql]
   [next.jdbc :as jdbc]
   [next.jdbc.connection :as connection]
   [next.jdbc.result-set :as rs]
   [next.jdbc.prepare :as prepare]
   [org.zsxf.zset :as zs]
   [org.zsxf.util :as util]
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
  (jdbc/execute! db-conn ["SELECT 1;"]))

(defn init
  "Initializes the database connection pool and checks the connection."
  []
  (init-db-connection-pool *db-conn-pool)
  (check-db-connection-pool! @*db-conn-pool))


(defn query->reducible [db-conn query]
  (jdbc/plan db-conn
    (hsql/format query)))

(defn table->reducible [db-conn fully-qualified-table-name]
  (jdbc/plan db-conn
    (hsql/format
      {:select [:*]
       :from   [fully-qualified-table-name]
       :where  [:<= :id 1000000]
       })))

(def table-row->zset-xf
  ; transform a table row into a zset
  (map (fn [row] (zs/zset #{row}))))

(defonce *all-teams (atom nil))
(defonce *all-players (atom nil))

(defn init-all-data []
  (init)
  (reset! *all-teams
    (a/<!!
      (a/reduce
        conj
        []
        (util/reducible->chan
          (query->reducible @*db-conn-pool
            {:select [:*]
             :from   [:saberstack.zsxf.team]
             :where  [:<= :id 100]})
          (a/chan 42 table-row->zset-xf)))))
  (reset! *all-players
    (a/<!!
      (a/reduce
        conj
        []
        (util/reducible->chan
          (query->reducible @*db-conn-pool
            {:select [:*]
             :from   [:saberstack.zsxf.player]
             :where  [:<= :id 1000000]})
          (a/chan 42 table-row->zset-xf)))))
  :done)

; demo only
(defonce *incremental-teams (atom #{}))                     ;set of zsets
(defonce *incremental-players (atom #{}))                   ;set of maps

(defn reset-incremental-data-state! []
  (reset! *incremental-teams #{})
  (reset! *incremental-players #{}))

(defn incremental-data []
  (init)
  (let [current-teams            (a/<!!
                                   (a/reduce conj #{}
                                     (util/reducible->chan
                                       (query->reducible @*db-conn-pool
                                         {:select [:*]
                                          :from   [:saberstack.zsxf.team]
                                          :where  [:> :id 100]})
                                       (a/chan 42 table-row->zset-xf))))
        current-players              (a/<!!
                                   (a/reduce conj #{}
                                     (util/reducible->chan
                                       (query->reducible @*db-conn-pool
                                         {:select [:*]
                                          :from   [:saberstack.zsxf.player]
                                          :where  [:> :id 1000000]})
                                       (a/chan 42 table-row->zset-xf))))
        prev-incremental-players @*incremental-players
        prev-incremental-teams   @*incremental-teams]

    (swap! *incremental-teams
      (fn [_s]
        current-teams))

    (swap! *incremental-players
      (fn [_s]
        current-players))

    {:new-teams       (clojure.set/difference @*incremental-teams prev-incremental-teams)
     :new-players     (clojure.set/difference @*incremental-players prev-incremental-players)
     :deleted-players (clojure.set/difference prev-incremental-players @*incremental-players)
     :deleted-teams   (clojure.set/difference prev-incremental-teams @*incremental-teams)}))

(comment
  ;TODO process all-teams and all-players through a join dataflow

  (a/<!!
    (a/reduce
      conj
      []
      (util/reducible->chan
        (table->reducible @*db-conn-pool :saberstack.zsxf.team)
        (a/chan 42 #_(a/dropping-buffer 10) (map (fn [row] row))))))

  (a/<!!
    (a/reduce
      conj
      []
      (util/reducible->chan
        (table->reducible @*db-conn-pool :saberstack.zsxf.player)
        (a/chan 42 #_(a/dropping-buffer 10) (map (fn [row] row)))))))
