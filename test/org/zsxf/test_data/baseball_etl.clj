(ns org.zsxf.test-data.baseball-etl
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [clojure.set :as set]
            [datascript.core :as d]))
;; SQLite database connection
(def db-spec {:dbtype "sqlite"
              :dbname "resources/baseball.sqlite"})

(def ds (jdbc/get-datasource db-spec))


(defn query [q]
  (jdbc/execute! ds (sql/format q)
                 {:builder-fn rs/as-unqualified-maps}))

(defn get-players [& {:keys [limit hall-of-fame-only?]}]
  (let [base-query (-> (h/select :p.playerID :p.nameFirst :p.nameLast
                                 :p.debut :p.finalGame :p.birthYear)
                       (h/from [:People :p]))]
    (cond-> base-query
      hall-of-fame-only? (-> (h/join [:HallOfFame :hof] [:= :p.playerID :hof.playerID])
                             (h/where [:= :hof.inducted "Y"]))
      limit (h/limit limit)
      true query)))

(defn get-teams [& {:keys [min-year max-year limit]}]
  (let [base-query (-> (h/select :teamID :name :yearID :lgID :franchID :divID :W :L)
                       (h/from :Teams))]
    (cond-> base-query
      min-year (h/where [:>= :yearID min-year])
      max-year (h/where [:<= :yearID max-year])
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

(def schema
  {:player/id {:db/cardinality :db.cardinality/one
               :db/unique :db.unique/identity}
   :player/first-name {}
   :player/last-name {}

   :team/id {:db/cardinality :db.cardinality/one
             :db/unique :db.unique/identity}
   :team/name {}

   :tenure/player {:db/type :db.type/ref}
   :tenure/team {:db/type :db.type/ref}
   :tenure/year {:db/cardinality :db.cardinality/many}})

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
   (comp (map (fn [[first-appearance & _ :as appearances]]
                (assoc first-appearance :years (map :yearID appearances))))
         (map (fn [{player-id :playerID team-id :teamID years :years :as row}]
                (let [{player-eid :db/id}  (d/entity @conn [:player/id player-id])
                      {team-eid :db/id}  (d/entity @conn [:team/id team-id])]
                      (def years years)
                  (when (and player-eid team-eid)
                    {:tenure/player player-eid
                     :tenure/team team-eid
                     :tenure/year years}))))
         (remove nil?)
         (partition-all 100))
   (completing (fn [_ batch]
                 (d/transact! conn batch)))
   nil
   (->> (get-appearances opts)
         (group-by (juxt :playerID :teamID))
         vals)))

(comment
  (time (do
          (let [opts {:min-year 1980
                      :max-year 1985}]
            (def conn (fresh-conn))
            (populate-datascript-db conn opts))))
  @conn
  (count (d/datoms @conn :aevt :tenure/year))

  )
