(ns org.zsxf.experimental.dataflow
  (:require [clojure.core.async :as a]
            [honey.sql :as hsql]
            [net.cgrand.xforms :as xforms]
            [next.jdbc :as jdbc]
            [org.zsxf.jdbc.postgres :as postgres]
            [org.zsxf.xf :as dbsp-xf]
            [org.zsxf.zset :as zs]
            [pangloss.transducers :as pxf]
            [taoensso.timbre :as timbre]
            [tech.v3.dataset :as ds]
            [org.zsxf.experimental.data :as exp-data]
            [clj-java-decompiler.core :as decompiler]))

(defonce *join-state (atom {}))
(defonce *grouped-by-state-team (atom {}))
(defonce *grouped-by-state-player (atom {}))

(defn pipeline-xf
  "Equivalent SQL join:

  SELECT * FROM zsxf.team t
  JOIN zsxf.player p ON t.id = p.team
  WHERE t.id = 20
---
  AND t.id < 21 AND p.id < 21

  (the last two conditions are for testing purposes)
  "
  []
  (comp
    (mapcat identity)
    (pxf/branch
      ;:team/id
      (comp
        (map (fn [m] #_(timbre/info "team!") m))
        (map (fn [m] (if (:team/id m) m {})))
        (pxf/cond-branch
          empty?
          (map identity)
          :team/id
          (comp
            (dbsp-xf/->where-xf (fn [m] (= 20 (:team/id m))))
            (dbsp-xf/->index-xf :team/id)
            (map (fn [grouped-by-result]
                   ;(timbre/spy grouped-by-result)
                   (swap! *grouped-by-state-team
                     (fn [m]
                       (zs/indexed-zset+ m grouped-by-result))))))))
      (comp
        (map (fn [m] #_(timbre/info "player!") m))
        (map (fn [m] (if (:player/team m) m {})))
        (pxf/cond-branch
          empty?
          (map identity)
          :player/team
          (comp
            (dbsp-xf/->index-xf :player/team)
            (map (fn [grouped-by-result]
                   ;(timbre/spy grouped-by-result)
                   (swap! *grouped-by-state-player
                     (fn [m]
                       (zs/indexed-zset+ m grouped-by-result)))))))))
    ;TODO join indexed-zsets
    (map (fn [j]
           ;(timbre/spy j)
           j
           ))))

(defonce *state (atom nil))

(defn reset-pipeline! []
  (reset! *join-state {})
  (reset! *grouped-by-state-team {})
  (reset! *grouped-by-state-player {})
  (let [from (a/chan 1)
        to   (a/chan (a/sliding-buffer 1)
               (map (fn [to-final] to-final)))]
    (a/pipeline 1
      to
      (pipeline-xf)
      from)
    (reset! *state [from to])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn init-from-memory []
  (let [[from to] @*state]
    ;(a/>!! from (exp-data/data1))
    ;(a/>!! from (exp-data/data2))
    (a/>!! from
      (zs/zset
        [
         ;{:team/id 3 :team/name "T3"}
         {:id 4 :team "A-dupe"}
         ;{:id 5 :team "B"}
         ;{:player-name "BP" :player/team 5}
         {:player-name "A1" :player/team 4}
         ;{:player-name "A2" :player/team 4}
         ]))))

(defn init-remove []
  (let [[from to] @*state]
    ;(a/>!! from (exp-data/data1))
    ;(a/>!! from (exp-data/data2))
    (a/>!! from
      (zs/zset-negative
        [
         {:id 4 :team "A-dupe"}
         ]))))

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


(defn init-from-postgres! []
  (reset-pipeline!)
  (let [[from to] @*state]
    ;(a/>!! from (exp-data/data1))
    ;(a/>!! from (exp-data/data2))
    (run!
      (fn [zset] (a/>!! from zset))
      (take 10000000
        @postgres/*all-teams)))

  (let [[from to] @*state]
    ;(a/>!! from (exp-data/data1))
    ;(a/>!! from (exp-data/data2))
    (run!
      (fn [zset] (a/>!! from zset))
      (take 10000000
        @postgres/*all-players))))

(comment
  (set! *print-meta* false)
  (set! *print-meta* true)
  (reset-pipeline!)
  (init-from-memory)
  (clojure.pprint/pprint
    @*grouped-by-state-team)
  (clojure.pprint/pprint
    @*grouped-by-state-player)
  (clojure.pprint/pprint
    (zs/join @*grouped-by-state-team @*grouped-by-state-player))
  (clojure.pprint/pprint
    @*join-state)

  (init-remove)
  )

; Scratch
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(comment

  (zs/indexed-zset*
    (zs/index
      (zs/zset [{:team "A" :id 1} {:team "Aa" :id 1} {:team "B" :id 2}])
      :id)
    (zs/index
      (zs/zset [{:player "R" :team 1} {:player "S" :team 2} {:player "T" :team 3}])
      :team))

  )

(comment
  (let [one   #{1}
        two   #{2}
        three #{3}
        four  #{4}
        five  #{5}
        m     {1 one 2 two 3 three 4 four 5 five}
        kfn   (comp odd? first)
        m'    (transduce (xforms/by-key kfn (xforms/into #{})) conj {} m)
        five' (second (some (fn [x] (when (= 5 (first x)) x)) (m' true)))]
    (identical? five' five)))

(comment
  ;complex column names seem to be supported
  (ds/row-map
    (ds/->>dataset [{#{:a 1} 1} {#{:a 2} 2}])
    (fn [row] (update-vals row inc)))
  )
