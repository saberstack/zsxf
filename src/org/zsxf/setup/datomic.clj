(ns org.zsxf.setup.datomic
  (:require [babashka.process :as process]
            [babashka.fs]))

(defn create-sqlite-db
  "Create SQLite database file at the specified path."
  [path]
  (process/shell "sqlite3" path "-init" "resources/datomic/sqlite_init.sql" ".exit"))

;TODO Continue here

; - create sqlite db
; - create transactor.properties config with sqlite path

;TODO playground
;
; - One Datomic installation with pre-loaded datasets
; - On-demand: Datomic Peers + ZSXF
