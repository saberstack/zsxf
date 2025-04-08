(ns org.zsxf.archive.experimental.datascript-scratch
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.zset :as zs]
            [org.zsxf.zset :as zset]
            [org.zsxf.xf :as xf]
            [datascript.core :as d]
            [org.zsxf.datascript :as ds :refer :all]
            [org.zsxf.util :as util :refer [nth2]]
            [taoensso.timbre :as timbre]))

(defn query-result-set-xf [result-set-state]
  (map (fn [result-set-delta]
         (timbre/spy result-set-delta)
         (swap! result-set-state
           (fn [m] (zs/zset-pos+ m result-set-delta))))))

(defn init-result [result result-delta]
  (if (nil? result)
    ;init
    (cond
      (map? result-delta) [{} zs/indexed-zset+]             ;for aggregates, allow negative weights
      (set? result-delta) [#{} zs/zset-pos+]                ;regular joins, no negative weight
      :else (throw (ex-info "result-delta must be either map or set"
                     {:result-delta result})))
    ;else, existing result
    (cond
      (and (map? result) (map? result-delta)) [result zs/indexed-zset+] ;for aggregates, allow negative weights
      (and (set? result) (set? result-delta)) [result zs/zset-pos+] ;regular joins no negative weights
      :else (throw (ex-info "result and result-delta together must be either maps or sets"
                     {:result result :result-delta result})))))

(defn query-result-state-xf
  "Saves query results to atom. Works for both zsets and indexed-zset results."
  [state]
  (map (fn [result-delta]
         (timbre/spy result-delta)
         (swap! state
           (fn [{:keys [result] :as state-m}]
             (let [[result result+] (init-result result result-delta)]
               (assoc state-m :result
                 (result+ result result-delta))))))))

(comment
  (let [conn (d/create-conn {})]
    [
     (:tx-data
      (d/transact!
        conn
        [{:person/name "Alice"}]))
     ;=> [#datascript/Datom[1 :person/name "Alice" 536870913 true]]
     ; Add initial fact

     (:tx-data
      (d/transact! conn
        [[:db/add 1 :person/name "Alice"]]))
     ;=> []
     ; ... empty because the fact already exists

     (:tx-data
      (d/transact! conn
        [[:db/add 1 :person/born "USA"]]))
     ;=> [#datascript/Datom[1 :person/born "USA" 536870915 true]]
     ; ... :tx-data contains the new facts
     ])
  )

(comment
  ;aggregates example
  ;
  (let [conn (d/create-conn {})]
    (d/transact! conn
      [{:team/name "A" :event/country "Japan" :team/points-scored 25}
       {:team/name "A" :event/country "Japan" :team/points-scored 18}
       {:team/name "A" :event/country "Australia" :team/points-scored 25}
       {:team/name "A" :event/country "Australia" :team/points-scored 1}])

    (d/q
      '[:find ?country (sum ?pts)
        :where
        [?e :team/name "A"]
        [?e :event/country ?country]
        [?e :team/points-scored ?pts]]
      @conn))
  )

(comment
  ;movie example
  (let [schema {:country/name   {:db/cardinality :db.cardinality/one
                                 :db/unique      :db.unique/identity}
                :person/id      {:db/cardinality :db.cardinality/one
                                 :db/unique      :db.unique/identity}
                :person/born    {:db/cardinality :db.cardinality/one
                                 :db/valueType   :db.type/ref}
                :movie/director {:db/cardinality :db.cardinality/one
                                 :db/valueType   :db.type/ref}}
        conn   (d/create-conn schema)]

    [(d/transact! conn
       [{:country/name "USA"}])

     (d/transact! conn
       [{:country/name "Germany"}])

     (d/transact! conn
       [{:country/name "Monaco"}])

     (d/transact! conn
       [{:person/id   1
         :person/name "Alice"
         :person/born [:country/name "USA"]}])

     (d/transact! conn
       [{:movie/director [:person/id 1]
         :movie/title    "RoboCop 1"}])

     (d/transact! conn
       [{:movie/director [:person/id 1]
         :movie/title    "RoboCop 2"}])

     (d/transact! conn
       [{:person/id   4
         :person/name "Bob"
         :likes       "Pizza"
         :person/born [:country/name "Monaco"]}])

     (d/transact! conn
       [{:movie/director [:person/id 4]
         :movie/title    "LOTR 1"}])

     (d/transact! conn
       [{:movie/director [:person/id 4]
         :movie/title    "LOTR 2"}])

     (d/transact! conn
       [{:movie/director [:person/id 4]
         :movie/title    "LOTR 3"}])

     ]

    @conn
    ;query
    (d/q
      '[:find (pull ?p [:person/name
                        {:person/born [:country/name]}])
        :where
        [?p :person/name ?name]
        [?m :movie/title _]
        [?m :movie/director ?p]]
      @conn)
    )

  )

(comment
  (let [schema {:team/name   {:db/cardinality :db.cardinality/one
                              :db/unique      :db.unique/identity}
                :player/name {:db/cardinality :db.cardinality/one
                              :db/unique      :db.unique/identity}
                :player/team {:db/cardinality :db.cardinality/one
                              :db/valueType   :db.type/ref}}
        ;        schema {}
        conn   (d/create-conn schema)]

    (d/transact! conn
      [{:team/name "T"}])
    (d/transact! conn
      [{:team/name "T2"}])

    (d/transact! conn
      [{:player/name "P"}])
    (d/transact! conn
      [{:player/name "P2"}])

    (d/transact! conn
      [{:player/name "P"
        :player/team [:team/name "T"]}])

    (d/q
      '[:find ?t
        ;:in $ ?team-name
        :where
        [?p :player/team ?t]
        [?t :team/name "T"]
        [?p :player/name "P"]]

      (comment
        (and
          [?p :player/team ?t])

        [?p :player/team [?t :team/name "T"] 0])

      @conn
      ;?team-name
      )
    ))

(comment
  ;case 1
  (let [schema {:person/friend {:db/cardinality :db.cardinality/many
                                :db/valueType   :db.type/ref}}
        conn   (d/create-conn schema)]

    [(d/transact! conn
       [{:person/name "Alice"}])
     (d/transact! conn
       [{:person/name "Bob"}])
     (d/transact! conn
       [{:db/id 1 :person/friend 2}])
     (d/transact! conn
       [{:db/id 1 :person/friend 1}])

     ;query
     (d/q
       '[:find ?v2
         :where
         [?p :person/name ?v]
         [?p :person/friend ?p2]
         [?p2 :person/name ?v2]]
       @conn)]
    ;query returns:
    ;=> #{["Alice"] ["Bob"]}
    )

  ;case 2 (notice the change in query)
  (let [schema {:person/friend {:db/cardinality :db.cardinality/many
                                :db/valueType   :db.type/ref}}
        conn   (d/create-conn schema)]

    [(d/transact! conn
       [{:person/name "Alice"}])
     (d/transact! conn
       [{:person/name "Bob"}])
     (d/transact! conn
       [{:db/id 1 :person/friend 2}])
     (d/transact! conn
       [{:db/id 1 :person/friend 1}])

     ;query
     (d/q
       '[:find ?v
         :where
         [?p :person/name ?v]
         [?p :person/friend ?p2]
         [?p2 :person/name ?v]]
       @conn)]
    ;query returns:
    ;=> #{["Alice"]}
    ))

(comment
  (let [schema {}
        conn   (d/create-conn schema)]

    [(d/transact! conn
       [{:person/name "Alice"}])
     (d/transact! conn
       [{:person/name "Bob"}])
     (d/transact! conn
       [{:person/name "Clark"}])
     (d/transact! conn
       [{:person/name "Alice"}])]

    ;query
    (d/q
      '[:find ?p1 ?name
        :where
        [?p1 :person/name ?name]
        [?p2 :person/name ?name]
        [(not= ?p1 ?p2)]
        ]
      @conn)
    )
  )

(comment

  (d/q
    '[:find ?e ?t
      ;:in $ ?team-name
      :where
      [?e :player/name]
      [?t :team/name]]
    @conn
    ;?team-name
    )

  (d/q
    '[:find ?e
      ;:in $ ?team-name
      :where
      [?e :player/team ?t]
      [?e :player/city "NY"]
      [?t :team/name "A"]
      [?t :team/color "red"]
      ]
    @conn
    ;?team-name
    )

  ;TODO how does DBSP compute a query like this one:
  (d/q
    '[:find ?e
      ;:in $ ?team-name
      :where
      [?e :player/team ?t]
      [?e :player/city "NY"]]
    @conn
    ;?team-name
    ))

(defonce index-state-all (atom {}))
(defonce result-deltas (atom []))
(defonce result-set (atom #{}))
(defonce result-state (atom nil))

(comment
  ;transform result set to datascript form
  (into #{}
    (map (fn [result-set-item] [(:db/id (first result-set-item))]))
    @result-set))

(defonce input (atom (a/chan)))

(defn print-index-state []
  (timbre/spy @index-state-all)
  nil)


(defn reset-state! []
  (do
    (reset! index-state-all {})
    (reset! result-deltas [])
    (reset! result-set #{})
    (reset! result-state nil)
    (reset! input (a/chan)))
  )
