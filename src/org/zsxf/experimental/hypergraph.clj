(ns org.zsxf.experimental.hypergraph
  (:require [clj-memory-meter.core :as mm]
            [datomic.api :as d]
            [ubergraph.core :as uber]
            [bifurcan-clj.graph :as g]
            [tech.v3.dataset :as ds]
            [ubergraph.alg :as uber.alg]))

;TODO Continue here
;; evaluate matrix hypergraph representations
;; rows: vertices
;; columns: hyperedges

(defn v3-hypergraph []
  (let [vertices (ds/->dataset {:id [1 2 3] :label ["A" "B" "C"]})
        hyperedges (ds/->dataset {:edge-id [1 2] :type ["group" "link"]})
        incidences (ds/->dataset {:vertex-id [1 1 2 3] :edge-id [1 2 2 2]})]
    ))

(comment
  (mm/measure
    (ds/->dataset {:n (reverse (range 1000000))}))

  (mm/measure
    (ds/->dataset {:n (repeatedly 1000000 (fn [] (rand-int 1000000)))}))

  (mm/measure (vec (range 1000000)))
  )

(defn init-hypergraph []
  (let [query1 '[:where
                 [?p :person]
                 [?c :city "NYC"]
                 [?p :person-lives-in-city ?c]]
        query2 '[:where
                 [?p :person]
                 [?c :city "NYC"]
                 [?p :person-lives-in-city ?c]
                 ;new clause
                 [?p :person-lives-in-district ?d]
                 [?d :district "Queens"]]]
    ))

(defn add-edge [g [src dest]]
  (uber/add-undirected-edges* g [[src dest]]))

(comment
  ;find edges
  (uber/find-edges
    (uber/graph
      [#d2 [1 :person/name "Alice"] #d2 [1 :person/country 2]])
    {:dest #d2 [1 :person/country 2]})
  )

(comment

  ;; Vertices of the hypergraph
  (def vertices [:v1 :v2 :v3 :v4])

  ;; Hyperedges of the hypergraph
  (def hyperedges {:e1 [:v1 :v2 :v3]
                   :e2 [:v2 :v3 :v4]})

  ;; Build the bipartite graph representation
  (def g
    (apply uber/build-graph
      (uber/graph)
      (for [[edge-name edge-vertices] hyperedges
            vertex edge-vertices]
        [vertex edge-name])))

  (uber/nodes g)
  (mapv (juxt :src :dest)
    (uber/edges g))
  )
(comment
  (let [g1 (uber/graph [:a :b] [:c :d])]
    (uber/pprint g1)))

(defn init-graph []
  (=
    (uber/graph
      [#d2 [1 :person/name "Alice"] #d2 [1 :person/country 2]]
      [#d2 [1 :person/country 2] #d2 [2 :country/name "USA"]])
    (uber/graph
      [#d2 [1 :person/name "Alice"] #d2 [1 :person/country 2]]
      [#d2 [2 :country/name "USA"] #d2 [1 :person/country 2]]))

  (mm/measure
    (uber/graph
      [#d2 [1 :person/name "Alice"] #d2 [1 :person/country 2]]
      [#d2 [2 :country/name "USA"] #d2 [1 :person/country 2]]
      [#d2 [2 :country/name "USA"] #d2 [2 :country/continent 3]]))

  (let [{:keys [nodes undirected-edges] :as g}
        (uber/graph
          [#d2 [1 :person/name "Alice"] #d2 [1 :person/country 2]]
          [#d2 [2 :country/name "USA"] #d2 [1 :person/country 2]])
        [[datom']] nodes
        [[datom'']] undirected-edges
        _  (uber/pprint g)
        g' (uber/add-undirected-edges* g [[#d2 [2 :country/name "USA"] #d2 [2 :country/continent 3]]])]
    (uber/pprint g')

    (= g'
      (uber/graph
        [#d2 [1 :person/name "Alice"] #d2 [1 :person/country 2]]
        [#d2 [2 :country/name "USA"] #d2 [1 :person/country 2]]
        [#d2 [2 :country/name "USA"] #d2 [2 :country/continent 3]]))

    )

  (let [g  (uber/graph
             [#d2 [1 :person/name "Alice"] #d2 [1 :person/country 2]]
             [#d2 [2 :country/name "USA"] #d2 [1 :person/country 2]])
        _  (uber/pprint g)
        g' (uber/add-nodes* g [#d2 [2 :country/continent 3]])]
    (uber/pprint g')
    (uber.alg/bipartite-sets g'))


  (mm/measure
    [
     [
      [#d2 [1 :person/name "Alice"] #d2 [1 :person/country 2]]

      #d2 [2 :country/name "USA"]
      ]
     #d2 [2 :country/continent 3]])
  )


(defn bifurcan-graph-init []
  (=
    (->
      (g/graph #(hash (name %)) (fn [a b] (= (name a) (name b))))
      (g/link :x :y)
      (g/link :x :y)
      ;(g/link "y" "x")
      ;(g/vertices)
      clojure.datafy/datafy)
    (->
      (g/graph #(hash (name %))
        (fn [a b]
          (println "called with" a b)
          (= (name a) (name b))))
      (g/link :x :y)
      (g/link :y :x)
      ;(g/link :x :y)
      ;(g/link "y" "x")
      ;(g/vertices)
      ;clojure.datafy/datafy
      ))

  )
