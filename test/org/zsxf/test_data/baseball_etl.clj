(ns org.zsxf.test-data.baseball-etl
  (:require [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [clojure.set :as set]
            [datascript.core :as d]
            [taoensso.timbre :as timbre]
            [org.zsxf.input.datascript :as ds]
            [org.zsxf.datalog.compiler :refer [static-compile]]
            [org.zsxf.query :as q]))
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

(deftest datascript-aggregates "datascript aggregate behavior"
  (let [conn (conn-for-range 1950 2000)
        agg-by-name '[:find ?player-name (sum ?home-runs)
                      :where
                      [?p :player/id "aaronha01"]
                      [?p :player/name ?player-name]
                      [?s :season/player ?p]
                      [?s :season/home-runs ?home-runs]
                      [?s :season/year ?year]]
        no-agg-no-year '[:find ?player-name ?home-runs
                         :where
                         [?p :player/id "aaronha01"]
                         [?p :player/name ?player-name]
                         [?s :season/player ?p]
                         [?s :season/home-runs ?home-runs]
                         [?s :season/year ?year]]
        no-agg-with-year '[:find ?player-name ?year ?home-runs
                           :where
                           [?p :player/id "aaronha01"]
                           [?p :player/name ?player-name]
                           [?s :season/player ?p]
                           [?s :season/home-runs ?home-runs]
                           [?s :season/year ?year]]]
    (testing "The aggregate underreports Aaron's true home run total."
        (is (= [["Hank Aaron" 510]] (d/q agg-by-name @conn))))
    (testing "The un-aggregated version reflects the same total."
      (is (= 510 (->> (d/q no-agg-no-year @conn)
                      (map second)
                      (reduce +)))))
    (testing "Including year in the result allows all home runs to be counted."
      (is (= 755 (->> (d/q no-agg-with-year @conn)
                      (map #(nth % 2))
                      (reduce +)))))
    (testing "ZSXF gives the \"expected\" behavior."
      (let [zquery (q/create-query
                    (static-compile '[:find ?player-name ?year  ?home-runs
                                      :where
                                      [?p :player/id "aaronha01"]
                                      [?p :player/name ?player-name]
                                      [?s :season/player ?p]
                                      [?s :season/home-runs ?home-runs]
                                      [?s :season/year ?year]]))]
        (ds/init-query-with-conn zquery conn)
        (is (= {["Hank Aaron"] #{['?home-runs 755]}}
               (q/get-aggregate-result zquery)))))))

(def za (atom []))
(def zquery (q/create-query
             (fn [state43087]
               (comp
                (org.zsxf.xf/mapcat-zset-transaction-xf)
                (map
                 (fn [zset__42554__auto__]
                   (org.zsxf.xf/disj-irrelevant-items
                    zset__42554__auto__
                    #(org.zsxf.datom/datom-attr= % :season/home-runs)
                    #(org.zsxf.datom/datom-attr-val= % :player/id "aaronha01")
                    #(org.zsxf.datom/datom-attr= % :season/player)
                    #(org.zsxf.datom/datom-attr= % :season/year)
                    #(org.zsxf.datom/datom-attr= % :player/name))))
                (org.zsxf.xf/join-xf
                 {:path identity,
                  :pred
                  #(org.zsxf.datom/datom-attr-val= % :player/id "aaronha01"),
                  :index-kfn org.zsxf.datom/datom->eid,
                  :clause '[?p :player/id "aaronha01"]}
                 {:path identity,
                  :pred #(org.zsxf.datom/datom-attr= % :player/name),
                  :index-kfn org.zsxf.datom/datom->eid,
                  :clause '[?p :player/name ?player-name]}
                 state43087)
                (org.zsxf.xf/join-xf
                 {:path org.zsxf.datalog.macro-util/safe-first,
                  :pred
                  #(org.zsxf.datom/datom-attr-val= % :player/id "aaronha01"),
                  :index-kfn org.zsxf.datom/datom->eid,
                  :clause '[?p :player/id "aaronha01"]}
                 {:path identity,
                  :pred #(org.zsxf.datom/datom-attr= % :season/player),
                  :index-kfn org.zsxf.datom/datom->val,
                  :clause '[?s :season/player ?p]}
                 state43087)
                (org.zsxf.xf/join-xf
                 {:path org.zsxf.datalog.macro-util/safe-second,
                  :pred #(org.zsxf.datom/datom-attr= % :season/player),
                  :index-kfn org.zsxf.datom/datom->eid,
                  :clause '[?s :season/player ?p]}
                 {:path identity,
                  :pred #(org.zsxf.datom/datom-attr= % :season/home-runs),
                  :index-kfn org.zsxf.datom/datom->eid,
                  :clause '[?s :season/home-runs ?home-runs]}
                 state43087)
                (org.zsxf.xf/join-xf
                 {:path
                  (comp
                   org.zsxf.datalog.macro-util/safe-second
                   org.zsxf.datalog.macro-util/safe-first),
                  :pred #(org.zsxf.datom/datom-attr= % :season/player),
                  :index-kfn org.zsxf.datom/datom->eid,
                  :clause '[?s :season/player ?p]}
                 {:path identity,
                  :pred #(org.zsxf.datom/datom-attr= % :season/year),
                  :index-kfn org.zsxf.datom/datom->eid,
                  :clause '[?s :season/year ?year]}
                 state43087
                 :last?
                 true)
                (net.cgrand.xforms/reduce
                 (org.zsxf.zset/zset-xf+
                  (map
                   (org.zsxf.xf/same-meta-f
                    (juxt
                     (comp
                      org.zsxf.datom/datom->val
                      org.zsxf.datalog.macro-util/safe-second
                      org.zsxf.datalog.macro-util/safe-first
                      org.zsxf.datalog.macro-util/safe-first
                      org.zsxf.datalog.macro-util/safe-first)
                     (comp
                      org.zsxf.datom/datom->val
                      org.zsxf.datalog.macro-util/safe-second)
                     (comp
                      org.zsxf.datom/datom->val
                      org.zsxf.datalog.macro-util/safe-second
                      org.zsxf.datalog.macro-util/safe-first))))))))))

(defn run-zquery []
  (reset! za [])
  (let [conn (conn-for-range 1950 2000)
        _ (ds/init-query-with-conn zquery conn)
        result (q/get-result zquery)]
    (ds/unlisten-all! conn)
    result))


(comment
  (def aaa (run-zquery))
  (#(nth % 2) (first aaa))

org.zsxf.zset/zset+
(set! *print-meta* false)
(org.zsxf.zset/zset+ (map (org.zsxf.xf/same-meta-f (juxt #(nth % 2))) ) #{} aaa)
(transduce (org.zsxf.zset/zset-xf+
             (map (org.zsxf.xf/same-meta-f (juxt #(nth % 2))) ))
           conj
           #{}
           aaa)
  (into #{} (org.zsxf.zset/zset-xf+
             (map (org.zsxf.xf/same-meta-f (juxt #(nth % 2))) )) aaa)#{^#:zset{:w 1024} ["Hank Aaron" 1958 30] ^#:zset{:w 1024} ["Hank Aaron" 1971 47] ^#:zset{:w 1024} ["Hank Aaron" 1974 20] ^#:zset{:w 1024} ["Hank Aaron" 1964 24] ^#:zset{:w 1024} ["Hank Aaron" 1972 34] ^#:zset{:w 1024} ["Hank Aaron" 1959 39] ^#:zset{:w 1024} ["Hank Aaron" 1969 44] ^#:zset{:w 1024} ["Hank Aaron" 1961 34] ^#:zset{:w 1024} ["Hank Aaron" 1967 39] ^#:zset{:w 1024} ["Hank Aaron" 1973 40] ^#:zset{:w 1024} ["Hank Aaron" 1956 26] ^#:zset{:w 1024} ["Hank Aaron" 1970 38] ^#:zset{:w 1024} ["Hank Aaron" 1975 12] ^#:zset{:w 1024} ["Hank Aaron" 1968 29] ^#:zset{:w 1024} ["Hank Aaron" 1954 13] ^#:zset{:w 1024} ["Hank Aaron" 1962 45] ^#:zset{:w 1024} ["Hank Aaron" 1963 44] ^#:zset{:w 1024} ["Hank Aaron" 1955 27] ^#:zset{:w 1024} ["Hank Aaron" 1957 44] ^#:zset{:w 1024} ["Hank Aaron" 1960 40] ^#:zset{:w 1024} ["Hank Aaron" 1976 10] ^#:zset{:w 1024} ["Hank Aaron" 1965 32] ^#:zset{:w 1024} ["Hank Aaron" 1966 44]}
  (map (org.zsxf.xf/same-meta-f (juxt #(nth % 2))) aaa)
  (transduce
   )
  ((net.cgrand.xforms/reduce
    (org.zsxf.zset/zset-xf+ (map #(nth % 2)))) aaa)
  (org.zsxf.zset/index aaa first)
  (count @za)

  (d/q '[:find ?player-name (sum ?home-runs)
         :where
         [?p :player/id "aaronha01"]
         [?p :player/name ?player-name]
         [?s :season/player ?p]
         [?s :season/home-runs ?home-runs]
         [?s :season/year ?year]]
       (-> (conn-for-range 1950 2000) deref)
   )
  (static-compile '[:find ?player-name (sum ?home-runs)
         :where
         [?p :player/id "aaronha01"]
         [?p :player/name ?player-name]
         [?s :season/player ?p]
         [?s :season/home-runs ?home-runs]
         [?s :season/year ?year]])

  org.zsxf.zset/zset-xf+
  (org.zsxf.zset/zset+ )
  org.zsxf.zset/index
  org.zsxf.xf/group-by-xf
  (static-compile '[:find ?player-name ?home-runs
         :where
         [?p :player/id "aaronha01"]
         [?p :player/name ?player-name]
         [?s :season/player ?p]
         [?s :season/home-runs ?home-runs]
         [?s :season/year ?year]])
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
