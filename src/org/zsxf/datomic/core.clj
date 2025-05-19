(ns org.zsxf.datomic.core
  (:require
   [datomic.api :as dd]))

(defn db-uri [db-name]
  (str "datomic:sql://" db-name "?jdbc:sqlite:./datomic/storage/sqlite.db"))

(defn get-database-schema [db-uri]
  (let [conn (dd/connect db-uri)]
    (dd/q
      '[:find ?attr ?type ?card
        :where
        [_ :db.install/attribute ?a]
        [?a :db/valueType ?t]
        [?a :db/cardinality ?c]
        [?a :db/ident ?attr]
        [?t :db/ident ?type]
        [?c :db/ident ?card]]
      (dd/db conn))))

(defn test-tx []
  (let [conn (dd/connect (db-uri "zsxf"))]
    (dd/transact conn
      [{:movie/title "The Matrix"}])))

(defn test-q []
  (let [conn (dd/connect (db-uri "zsxf"))]
    (dd/q
      '[:find ?e ?v
       :where [?e :movie/title ?v]]
      (dd/db conn))))

(comment

  (dd/create-database (db-uri "zsxf"))

  (dd/get-database-names (db-uri "*"))

  (dd/connect (db-uri "zsxf"))

  (get-database-schema (db-uri "zsxf"))

  (let [conn (dd/connect (db-uri "zsxf"))]
    (dd/transact conn [{:db/ident       :movie/title
                        :db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one
                        :db/doc         "The title of the movie"}]))

  (let [conn (dd/connect (db-uri "zsxf"))]
    (dd/q
      '[:find ?e ?v
        :where [?e :movie/title ?v]]
      (dd/db conn)))


  )
