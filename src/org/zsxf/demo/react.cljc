(ns org.zsxf.demo.react
  (:require [clj-memory-meter.core :as mm]
            [datascript.core :as d]
            [org.zsxf.datalog.compiler :refer [static-compile]]
            [org.zsxf.datascript :as ds]
            [org.zsxf.query :as q]))


(def schema
  {:cell/id   {:db/cardinality :db.cardinality/one
               :db/unique      :db.unique/identity}
   :input/id  {:db/cardinality :db.cardinality/one
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

(defn get-result [conn query param]
  (d/transact! conn [{:input/id param}])
  (let [return (q/get-result query)]
    (d/transact! conn [[:db/retractEntity [:input/id param]]])
    return))

(comment

  (init-queries)

  (init 100000)

  (mm/measure *conn)

  ;(let [query (q/create-query
  ;              (static-compile
  ;                '[:find ?d
  ;                  :where
  ;                  [?e :cell/id 3]
  ;                  [?e :cell/data ?d]]))]
  ;  (def query query)
  ;  (ds/init-query-with-conn query @*conn))

  (let [query-all (q/create-query
                    (static-compile
                      '[:find ?d
                        :where
                        [?e :cell/data ?d]
                        [?e :cell/id ?cell-id]
                        [?p :input/id ?cell-id]]))]
    (def query-all query-all)
    (ds/init-query-with-conn query-all @*conn))

  (q/get-result query-all)

  (time
    (get-result @*conn query-all (rand-int 100000)))

  (dotimes [_ 100]
    (time
      (get-result @*conn query-all (rand-int 100000))))

  (dotimes [_ 100]
    (time
      (d/q
        '[:find ?d
          :in $ ?cell-id
          :where
          [?e :cell/id ?cell-id]
          [?e :cell/data ?d]]
        @@*conn
        (rand-int 100000))))


  (time
    (get-result @*conn query-all 1000))

  (org.zsxf.alpha.repl/sample-indices query-all)

  (mm/measure query-all)

  (d/q
    '[:find ?cell-id
      :where
      [?e :cell/data ?d]
      [?e :cell/id ?cell-id]
      [?p :input/param ?param-value]
      [(= ?cell-id ?param-value)]]
    @@*conn)

  (time
    (d/transact! @*conn [{:input/param 3}]))

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
