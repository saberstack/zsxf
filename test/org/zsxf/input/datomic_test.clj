(ns org.zsxf.input.datomic-test
  (:require [clojure.test :refer [deftest is testing]]
            [datomic.api :as dd]
            [org.zsxf.datomic.cdc :as dcdc]
            [org.zsxf.setup.datomic :as setup-datomic]
            [taoensso.timbre :as timbre]))

(defn conn! [db-name]
  (try
    (let [sqlite-path "/tmp/storage/sqlite.db"
          ;_           (setup-datomic/create-sqlite-db sqlite-path)
          uri         (dcdc/uri-sqlite db-name sqlite-path)
          _           (timbre/info uri)
          ;_              (dd/delete-database db-uri)
          ;ret         (timbre/spy (dd/create-database uri))
          conn        (dd/connect uri)
          ]
      conn)
    (catch Throwable e e)))

(deftest conn-check
  (is
    (true?
      (dcdc/conn?
        (transduce
          (comp
            (map (fn [_conn-attempt] (conn! "app")))
            (map (fn [conn-or-throwable]
                   (timbre/info conn-or-throwable)
                   conn-or-throwable))
            (halt-when
              (fn [conn-or-throwable]
                (timbre/info (type conn-or-throwable))
                (not (instance? Throwable conn-or-throwable))))
            (map (fn [_] (Thread/sleep 1000))))
          conj
          (range 10))))))
