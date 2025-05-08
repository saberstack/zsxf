(ns org.zsxf.demo.react
  (:require [datascript.core :as d]))


(def schema
  {:cell/id   {:db/cardinality :db.cardinality/one
               :db/unique      :db.unique/identity}
   :cell/data {}})

(defn gen-cells [n]
  (into []
    (comp
      (map (fn [id]
             {:cell/id   id
              :cell/data ["cell id" id]})))
    (range n)))

(defonce *conn (atom nil))

(defn init []
  (let [conn (d/create-conn schema)
        data (gen-cells 10000)]

    (reset! *conn conn)
    (d/transact! conn data))
  )

(defn render-cells []
  (time
    (do (d/q
          '[:find ?d
            :where
            [?e :cell/id 55]
            [?e :cell/data ?d]]
          @@*conn)
      :done)))
