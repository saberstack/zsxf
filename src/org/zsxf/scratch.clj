(ns org.zsxf.scratch
  (:require [clojure.core.async :as a]
            [datascript.core :as d]))

(defonce *conn (atom nil))

(def schema
  {:person/uuid  {:db/cardinality :db.cardinality/one
                  :db/unique      :db.unique/identity}
   :person/name  {:db/cardinality :db.cardinality/one}

   :team/uuid    {:db/cardinality :db.cardinality/one
                  :db/unique      :db.unique/identity}
   :team/name    {:db/cardinality :db.cardinality/one
                  :db/unique      :db.unique/value}
   :team/persons {:db/cardinality :db.cardinality/many
                  :db/unique      :db.unique/value
                  :db/valueType   :db.type/ref}})

(defn init []
  (let [schema {:aka {:db/cardinality :db.cardinality/many}}
        conn   (reset! *conn (d/create-conn schema))
        tx     (d/transact! conn [{:db/id -1
                                   :name  "Max"
                                   :age   45
                                   :aka   ["Max Otto von Stierlitz", "Jack Ryan"]}])]
    (d/q '[:find ?n ?a
           :where
           [?e :aka "Max Otto von Stierlitz"]
           [?e :name ?n]
           [?e :age ?a]]
      @conn)))

(defn init-2 []
  (let [conn (reset! *conn (d/create-conn schema))
        tx   (d/transact! conn [{:db/id       -1
                                 :person/uuid (random-uuid)
                                 :person/name "Max V"}])]
    tx))

(defn init-f1-data []
  (let [team-1   (random-uuid)
        team-2   (random-uuid)
        person-1 (random-uuid)
        person-2 (random-uuid)
        person-3 (random-uuid)
        person-4 (random-uuid)
        person-5 (random-uuid)]

    (reset! *conn (d/create-conn schema))

    (d/transact @*conn [{:team/uuid team-1 :team/name "Ferrari"}])
    (d/transact @*conn [{:team/uuid team-2 :team/name "Red Bull"}])

    (d/transact @*conn [{:person/uuid person-1 :person/name "Lewis H"}])
    (d/transact @*conn [{:person/uuid person-2 :person/name "Sergio P"}])
    (d/transact @*conn [{:person/uuid person-3 :person/name "Charles L"}])
    (d/transact @*conn [{:person/uuid person-4 :person/name "Carlos S"}])
    (d/transact! @*conn [{:person/uuid person-5 :person/name "Max V"}])

    (d/transact @*conn [{:team/uuid team-1 :team/persons [:person/uuid person-3]}
                        {:team/uuid team-1 :team/persons [:person/uuid person-4]}
                        {:team/uuid team-2 :team/persons [:person/uuid person-2]}]))

  )

(defn find-team [team-name]
  (d/q '[:find (pull ?e [*])
         :in $ ?team-name
         :where
         [?e :team/name ?team-name]]
    @@*conn
    team-name))

(defn find-team-people [team-name]
  (d/q '[:find (pull ?persons [*]) #_(pull ?persons [*])
         :in $ ?team-name
         :where
         [?e :team/name ?team-name]
         [?e :team/persons ?persons]
         ]
    @@*conn
    team-name))

(defn find-person-with-name-in-a-team [team-name person-name]
  (d/q '[:find ?person-name #_(pull ?p [*])
         :in $ ?team-name ?person-name
         :where
         [?e :team/name ?team-name]
         [?e :team/persons ?p]
         [?p :person/name ?person-name]]
    @@*conn
    team-name
    person-name))

(defn find-person-by-name [person-name]
  (d/q '[:find (pull ?p [*])
         :in $ ?person-name
         :where
         [?p :person/name ?person-name]]
    @@*conn
    person-name))

(defn count-people
  "Count people in the database"
  []
  (ffirst
    (d/q '[:find (count ?e)
           :where [?e :person/uuid]]
      @@*conn)))

(defn find-person-with-name-in-a-team-xf [team-name person-name]
  ;TODO Continue here
  (comp
    (filter (fn [x] (= (:team/name x) team-name)))
    (filter (fn [x] (= (:team/name x) team-name)))
    ))

(defn add-new-team-person [team-uuid person-uuid]
  (d/transact @*conn [{:team/uuid    team-uuid
                       :team/persons [:person/uuid person-uuid]}]))
;; transaction data/delta
;;  :tx-data [#datascript/Datom[1 :team/persons 3 536870921 true]],

(defn rename-team []
  (d/transact @*conn [{:team/uuid #uuid"81c3634f-d825-4f76-97e7-21f6481c7df6"
                       :team/name "Scuderia Ferrari"}]))

;:tx-data [#datascript/Datom[1 :team/name "Ferrari" 536870922 false]
;          #datascript/Datom[1 :team/name "Scuderia Ferrari" 536870922 true]],

(defn tx-2 []
  (d/transact! @*conn
    [{:db/id -1
      :name  "RS"
      :age   999
      :aka   ["ne0"]}]))

(defn tx-3 []
  )


(defn core-async-map []
  (let [ch-1 (a/chan 100 (filter even?))
        ch-2 (a/chan 100 (filter even?))]

    (a/>!! ch-1 42)
    (a/>!! ch-2 43)
    (a/>!! ch-2 44)
    (a/>!! ch-2 46)

    (a/<!! (a/map vector [ch-1 ch-2]))))


(comment
  #datascript.db.TxReport{:db-before #datascript/DB{:schema {:aka #:db{:cardinality :db.cardinality/many}},
                                                    :datoms [[1 :age 45 536870913]
                                                             [1 :aka "Jack Ryan" 536870913]
                                                             [1 :aka "Max Otto von Stierlitz" 536870913]
                                                             [1 :name "Max" 536870913]]},
                          :db-after  #datascript/DB{:schema {:aka #:db{:cardinality :db.cardinality/many}},
                                                    :datoms [[1 :age 45 536870913]
                                                             [1 :aka "Jack Ryan" 536870913]
                                                             [1 :aka "Max Otto von Stierlitz" 536870913]
                                                             [1 :name "Max" 536870913]
                                                             [2 :age 999 536870914]
                                                             [2 :aka "neo" 536870914]
                                                             [2 :name "RS" 536870914]]},
                          :tx-data   [#datascript/Datom[2 :name "RS" 536870914 true]
                                      #datascript/Datom[2 :age 999 536870914 true]
                                      #datascript/Datom[2 :aka "neo" 536870914 true]],
                          :tempids   {-1 2, :db/current-tx 536870914},
                          :tx-meta   nil})
