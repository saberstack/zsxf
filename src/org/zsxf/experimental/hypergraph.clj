(ns org.zsxf.experimental.hypergraph
  (:require [clj-memory-meter.core :as mm]
            [ubergraph.core :as uber]
            [bifurcan-clj.graph :as g]
            [tech.v3.dataset :as ds]
            [ubergraph.alg :as uber.alg]))

;TODO Continue here
;; evaluate matrix hypergraph representations
;; rows: vertices
;; columns: hyperedges

(defn init-hypergraph []

  )

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
