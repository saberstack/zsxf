(ns org.zsxf.datomic.delta
  (:require [org.zsxf.datomic.core :as dc]
            [datomic.api :as dd]))


(comment

  (let [uri  (dc/db-uri "mbrainz")
        conn (dd/connect uri)]
    (dc/get-all-idents conn)))
