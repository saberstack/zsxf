(ns org.zsxf.test-data.baseball-etl
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [clojure.set :as set]
            [datascript.core :as d]
            [taoensso.timbre :as timbre]))
;; SQLite database connection
(def db-spec {:dbtype "sqlite"
              :dbname "resources/baseball.sqlite"})

(def ds (jdbc/get-datasource db-spec))

(defn query [q]
  (jdbc/execute! ds (sql/format q)
                 {:builder-fn rs/as-unqualified-maps}))

(defn get-players [& {:keys [limit hall-of-fame-only? min-year max-year]}]
  (let [base-query (-> (h/select :p.playerID :p.nameFirst :p.nameLast
                                 :p.debut :p.finalGame :p.birthYear)
                       (h/from [:People :p]))]
    (cond-> base-query
      min-year (h/where [:or [:>= :p.debut min-year] [:>= :p.finalGame min-year]])
      max-year (h/where [:or [:< :p.debut max-year] [:< :p.finalGame max-year]])
      hall-of-fame-only? (-> (h/join [:HallOfFame :hof] [:= :p.playerID :hof.playerID])
                             (h/where [:= :hof.inducted "Y"]))
      limit (h/limit limit)
      true query)))

(defn get-teams [& {:keys [min-year max-year limit]}]
  (let [base-query (-> (h/select :teamID :name :yearID :lgID :franchID :divID :W :L)
                       (h/from :Teams))]
    (cond-> base-query
      min-year (h/where [:>= :yearID min-year])
      max-year (h/where [:< :yearID max-year])
      limit (h/limit limit)
      true query)))

;; Query the Appearances table to connect players and teams
(defn get-appearances [& {:keys [min-year max-year player-ids team-ids limit]}]
  (let [base-query (-> (h/select :a.playerID :a.teamID :a.yearID)
                       (h/from [:Appearances :a])
                       (h/order-by :playerID :teamID :yearID))]
    (cond-> base-query
      min-year (h/where [:>= :a.yearID min-year])
      max-year (h/where [:<= :a.yearID max-year])
      player-ids (h/where [:in :a.playerID player-ids])
      team-ids (h/where [:in :a.teamID team-ids])
      limit (h/limit limit)
      true query)))

;; Get batting statistics
;; Note: We intentionally ignore the stint column. In rare cases (76 in baseball history),
;; a player may have multiple stints with the same team in one year. We just take the
;; first stint's data for simplicity. This won't affect home run leader calculations
;; meaningfully, as the cases are rare and the differences are small.
(defn get-batting [& {:keys [min-year max-year player-ids team-ids limit]}]
  (let [base-query (-> (h/select :playerID :teamID :yearID :HR
                                 ;:G :AB :R :H
                                 ;[:2B :doubles]
                                 ;[:3B :triples]
                                 ;:RBI :SB :CS :BB :SO
                                 )
                       (h/from :Batting)
                       ;; Just order by player/team/year, ignoring stint
                       (h/order-by :playerID :teamID :yearID))]
    (cond-> base-query
      min-year (h/where [:>= :yearID min-year])
      max-year (h/where [:< :yearID max-year])
      player-ids (h/where [:in :playerID player-ids])
      team-ids (h/where [:in :teamID team-ids])
      limit (h/limit limit)
      true query)))

(def schema
  {:player/id {:db/cardinality :db.cardinality/one
               :db/unique :db.unique/identity}
   :player/name {}

   :team/id {:db/cardinality :db.cardinality/one
             :db/unique :db.unique/identity}
   :team/name {}

   :season/player {:db/type :db.type/ref}
   :season/team {:db/type :db.type/ref}
   :season/year {}
   :season/home-runs {}})

(defn fresh-conn []
  (d/create-conn schema))

(defn populate-datascript-db [conn opts]
  (do
    (transduce
     (comp (map #(set/rename-keys % {:teamID :team/id
                                     :name :team/name
                                     :yearID :team/year
                                     :lgID :league/id
                                     :franchID :franchise/id
                                     :divID :division/id
                                     :W :team/wins
                                     :L :team/losses}))
           (map #(select-keys % [:team/id :team/name])))

     (fn ([acc el] (conj acc el))
       ([team-set] (d/transact! conn (vec team-set))))
     #{}
     (get-teams opts))

    (transduce
     (comp (map (fn [{:keys [playerID nameFirst nameLast]}]
                  {:player/id playerID
                   :player/name (if nameFirst
                                  (format "%s %s" nameFirst nameLast)
                                  nameLast)}))
           (partition-all 100))
     (completing  (fn [_ batch]
                    (d/transact! conn batch)) )
     nil
     (get-players opts)))

  (transduce
   (comp (map (fn [{player-id :playerID team-id :teamID year :yearID home-runs :HR :as row}]
                (let [{player-eid :db/id}  (d/entity @conn [:player/id player-id])
                         {team-eid :db/id}  (d/entity @conn [:team/id team-id])]
                     (when (and player-eid team-eid)
                       {:season/player player-eid
                        :season/team team-eid
                        :season/year year
                        :season/home-runs home-runs}))))
         (remove nil?)
         (partition-all 100))
   (completing (fn [_ batch]
                 (d/transact! conn batch)))
   nil
   (get-batting opts)))

(def conn-registry (atom {}))

(defn conn-for-range [low high]
  (or (get @conn-registry [low high])
      (let [conn (fresh-conn)]
        (populate-datascript-db conn {:min-year low :max-year high})
        (swap! conn-registry assoc [low high] conn)
        conn)))

(comment
  (time (do
          (let [opts {:min-year 1980
                      :max-year 1985}]
            (def conn (fresh-conn))
            (populate-datascript-db conn opts))))

  (count (d/datoms @conn :aevt :tenure/year))

  (doseq [range [[1980 1985]
                 [1975 2000]
                 [1950 2000]
                 [1900 2000]]]
    (let [conn (apply conn-for-range range)]
      (timbre/infof ("Range %s has %s datoms" range (count @conn)))
      (time (d/q '[:find ?player ?team ?year
                   :where
                   [?p :player/name ?player]
                   [?ten :tenure/player ?p]
                   [?ten :tenure/team ?t]
                   [?ten :tenure/year ?year]
                   [?t :team/name ?team]
                   :order ?year]
                 @conn))))

  )
