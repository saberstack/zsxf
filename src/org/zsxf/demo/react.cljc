(ns org.zsxf.demo.react
  (:require [datascript.core :as d]
            [org.zsxf.datalog.compiler :refer [static-compile]]
            [org.zsxf.datascript :as ds]
            [org.zsxf.query :as q]))


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

(defn init [n]
  (let [conn (d/create-conn schema)
        data (gen-cells n)]
    (reset! *conn conn)
    (d/transact! conn data)))

(defn get-cell-data [cell-id]
  (do
    (d/q
      '[:find ?d
        :in $ ?cell-id
        :where
        [?e :cell/id ?cell-id]
        [?e :cell/data ?d]]
      @@*conn
      cell-id)))

(defn set-cell-data! [cell-id data]
  (d/transact! @*conn
    [{:cell/id   cell-id
      :cell/data data}]))

(defmacro state-queries [n]
  (transduce
    (comp
      (map (fn [cell-id]
             (let [query [:find '?d
                          :where
                          ['?e :cell/id cell-id]
                          ['?e :cell/data '?d]]]
               `(static-compile '~query)))))
    conj
    (range n)))


(defonce *queries (atom []))

(defn init-queries []
  (reset! *queries [])
  (transduce
    (comp
      (map q/create-query)
      (map (fn [query]
             (swap! *queries conj query)
             query))
      (map (fn [query] (ds/init-query-with-conn query @*conn))))
    conj
    (state-queries 500)))

(comment

  (init 500)

  (init-queries)

  (let [query (q/create-query
                (static-compile
                  '[:find ?d
                    :where
                    [?e :cell/id 3]
                    [?e :cell/data ?d]]))]
    (def query query)
    (ds/init-query-with-conn query @*conn))

  (set-cell-data! 3 "new data")

  (dotimes [_ 10]
    (time
      (dotimes [cell-id 500]
        (get-cell-data cell-id))))

  (dotimes [_ 10]
    (time
      (dotimes [cell-id 500]
        (q/get-result
          (nth @*queries cell-id)))))

  (time
    (dotimes [cell-id 500]
      (set-cell-data! cell-id "500!")))

  )
