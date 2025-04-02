(ns org.zsxf.archive.experimental.postgres-data-init
  (:require [honey.sql :as hsql]
            [next.jdbc :as jdbc]
            [org.zsxf.zset :as zs]))

(defn data1 []
  (zs/zset
    [{:team/id 1 :team/name "T1"}
     {:team/id 2 :team/name "T2"}
     {:player/name "Alice" :player/team 1}
     {:player/name "Bob" :player/team 2}
     {:player/name "Chris" :player/team 2}]))

(defn data2 []
  (zs/zset
    [{:team/id 3 :team/name "T3"}
     {:team/id 4 :team/name "T4"}
     {:player/name "A" :player/team 3}
     {:player/name "B" :player/team 3}
     {:player/name "C" :player/team 4}]))


(defn init-postgres-queries []
  [
   ;database and schema
   "CREATE DATABASE saberstack;"
   "CREATE SCHEMA zsxf;"
   ;experimental 'team' table
   "CREATE TABLE zsxf.team (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                         name text NOT NULL);
   CREATE UNIQUE INDEX team_pkey ON zsxf.team(id int4_ops);"
   ;experimental 'player' tables
   "CREATE TABLE zsxf.player (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                           last_name text NOT NULL,
                                           team integer NOT NULL REFERENCES zsxf.team(id));
   CREATE UNIQUE INDEX player_pkey ON zsxf.player(id int4_ops);"
   ])

(defn init-postgres-data! [db-conn table-name row-maps]
  (jdbc/execute! db-conn
    (hsql/format
      {:insert-into table-name
       :values      row-maps
       ;:returning   :*
       })))

(defn generate-postgres-data-teams []
  (transduce
    (map (fn [idx] {:name (str "Team " idx)}))
    conj
    []
    (range 1 101)))

(defn generate-postgres-data-players [idx-multiplier]
  (transduce
    (map (fn [idx]
           (let [idx' (* idx-multiplier idx)]
             {:last_name (str "Player " idx') :team (inc (rand-int 100))})))
    conj
    []
    (range 1 25001)))

(comment
  ;write teams
  (init-postgres-data!
    @postgres/*db-conn-pool "saberstack.zsxf.team"
    (generate-postgres-data-teams))

  ;write players
  (run!
    (fn [idx-multiplier]
      (init-postgres-data!
        @postgres/*db-conn-pool "saberstack.zsxf.player"
        (generate-postgres-data-players idx-multiplier)))
    (range 1 1000)))
