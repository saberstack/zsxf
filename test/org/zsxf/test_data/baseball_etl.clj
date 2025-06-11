(ns org.zsxf.test-data.baseball-etl
  (:require [clojure.test :refer [deftest testing is are]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [clojure.set :as set]
            [datascript.core :as d]
            [org.zsxf.query :as q]
            [org.zsxf.datalog.compiler :refer [static-compile]]
            [org.zsxf.input.datascript :as ds]
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

   :season/id {:db/cardinality :db.cardinality/one
               :db/unique :db.unique/identity}
   :season/player {:db/type :db.type/ref}
   :season/team {:db/type :db.type/ref}
   :season/year {}
   :season/home-runs {}})

(defn fresh-conn []
  (d/create-conn schema))

(defn populate-datascript-db [conn opts]
  (let [exclude-attrs (or (:exclude-attrs opts) #{})]
        (transduce
           (comp (map #(set/rename-keys % {:teamID :team/id
                                           :name :team/name
                                           :yearID :team/year
                                           :lgID :league/id
                                           :franchID :franchise/id
                                           :divID :division/id
                                           :W :team/wins
                                           :L :team/losses}))
                 (map #(select-keys % (set/difference #{:team/id :team/name} exclude-attrs))))
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
             (map #(select-keys % (set/difference #{:player/id :player/name} exclude-attrs)))
             (partition-all 100))
       (completing  (fn [_ batch]
                      (d/transact! conn batch)) )
       nil
       (get-players opts))

    (transduce
      (comp (map (fn [{player-id :playerID team-id :teamID year :yearID home-runs :HR :as row}]
                   (let [{player-eid :db/id}  (d/entity @conn [:player/id player-id])
                         {team-eid :db/id}  (d/entity @conn [:team/id team-id])]
                     (when (and player-eid team-eid)
                       {:season/id (format "%s-%s-%s" player-id team-id year)
                        :season/player player-eid
                        :season/team team-eid
                        :season/year year
                        :season/home-runs home-runs}))))
            (remove nil?)
            (partition-all 100))
      (completing (fn [_ batch]
                    (d/transact! conn batch)))
      nil
      (get-batting opts))

    conn))

(def conn-registry (atom {}))

(defn conn-for-range [low high]
  (or (get @conn-registry [low high])
      (let [conn (fresh-conn)]
        (populate-datascript-db conn {:min-year low :max-year high})
        (swap! conn-registry assoc [low high] conn)
        conn)))


(deftest test-late-identifier
  (testing "When aggregate keys come in late"
    (let [conn (-> (fresh-conn)
                   (populate-datascript-db {:min-year 1980 :max-year 1985
                                            :exclude-attrs #{:team/name}}))
          zquery (q/create-query
                  (static-compile '[:find ?team (sum ?home-runs)
                                    :where
                                    [?t :team/name ?team]
                                    [?s :season/team ?t]
                                    [?s :season/player ?p]
                                    [?s :season/year ?year]
                                    [?s :season/home-runs ?home-runs]]))
          _ (ds/init-query-with-conn zquery conn)
          init-result (q/get-result zquery)
          _ (populate-datascript-db conn {:min-year 1980 :max-year 1985})
          ]
      (is (= (d/q '[:find ?team  (sum ?home-runs)
                    :with ?year ?p
                    :where
                    [?t :team/name ?team]
                    [?s :season/team ?t]
                    [?s :season/player ?p]
                    [?s :season/year ?year]
                    [?s :season/home-runs ?home-runs]]
                  @conn)
             (for [[[team] hrs]  (-> (q/get-aggregate-result zquery)
                                     (update-vals (comp second first)))]
               [team hrs]))))))

(deftest compound-aggregate
  (testing "When compound aggregate keys come in late"
    (let [conn (-> (fresh-conn)
                   (populate-datascript-db {:min-year 1980 :max-year 1985
                                            :exclude-attrs #{:team/name :player/name}}))
          init-attrs (->> conn deref :aevt (map second) (into #{}))
          q '[:find ?team ?player (sum ?home-runs)
              :with ?year
              :where
              [?t :team/name ?team]
              [?p :player/name ?player]
              [?s :season/team ?t]
              [?s :season/player ?p]
              [?s :season/year ?year]
              [?s :season/home-runs ?home-runs]]
          ;; The queries differ only by :with semantics,
          ;; because we haven't implemented them yet.
          zquery (q/create-query
                  (static-compile '[:find ?team ?player (sum ?home-runs)
                                    :where
                                    [?t :team/name ?team]
                                    [?p :player/name ?player]
                                    [?s :season/team ?t]
                                    [?s :season/player ?p]
                                    [?s :season/year ?year]
                                    [?s :season/home-runs ?home-runs]]))]
      (ds/init-query-with-conn zquery conn)
      (testing "There are no initial query results"
        (is (= {} (q/get-result zquery)))
        (is (= [] (d/q q @conn))))

      (populate-datascript-db conn {:min-year 1980 :max-year 1985
                                    :exclude-attrs #{:player/name}})

      (testing "The database now has :team/name attrs"
        (let [attrs-2 (->> conn deref :aevt (map second) (into #{}))]
          (is (= #{:team/name} (set/difference attrs-2 init-attrs)))))
      (testing "But still, the query results are empty."
        (is (= {} (q/get-result zquery)))
        (is (= [] (d/q q @conn))))


      (populate-datascript-db conn {:min-year 1980 :max-year 1985})

      (testing "The database now has :team/name attrs"
        (let [attrs-3 (->> conn deref :aevt (map second) (into #{}))]
          (is (= #{:team/name :player/name} (set/difference attrs-3 init-attrs)))))

      (testing "Now the ZSXF and datascript results match exactly."
        (is (= (for [[[team player] hrs] (-> (q/get-aggregate-result zquery)
                                             (update-vals (comp second first)))]
                 [team player hrs])
               (d/q q
                    @conn)))))))

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
