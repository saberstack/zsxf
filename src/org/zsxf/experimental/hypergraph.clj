(ns org.zsxf.experimental.hypergraph
  (:require [clj-memory-meter.core :as mm]
            [datomic.api :as d]
            [taoensso.timbre :as timbre]
            [ubergraph.core :as uber]
            [bifurcan-clj.graph :as g]
            [org.zsxf.type.zset :as zs]
            [tech.v3.dataset :as ds]
            [ubergraph.alg :as uber.alg])
  (:import (clojure.lang IPersistentMap)
           (org.zsxf.type.zset ZSet)))



(defprotocol IHypergraph
  "Defines the core operations for a hypergraph data structure."
  (add-vertex [this v] "Adds a vertex to the hypergraph. Returns the new hypergraph.")
  (add-hyperedge [this hedge] "Adds a hyperedge (a set of vertices) to the hypergraph. Returns the new hypergraph.")
  (remove-vertex [this v] "Removes a vertex and its appearances in any hyperedges. Returns the new hypergraph.")
  (remove-hyperedge [this hedge-id] "Removes a hyperedge by its ID. Returns the new hypergraph.")
  (get-vertices [this] "Returns a set of all vertices in the hypergraph.")
  (get-hyperedges [this] "Returns a map of all hyperedges (ID -> set of vertices).")
  (get-edges-for-vertex [this v] "Returns a set of hyperedge IDs containing the given vertex.")
  (get-vertices-for-edge [this hedge-id] "Returns a set of vertices contained in the given hyperedge.")
  (get-neighbors [this v] "Returns a set of all vertices that share a hyperedge with the given vertex (excluding itself).")
  (incident? [this v hedge-id] "Returns true if vertex v is a member of the hyperedge with hedge-id, false otherwise."))

(defrecord Hypergraph [vertices-to-edges edges-to-vertices next-edge-id]
  IHypergraph
  (add-vertex [this v]
    (update-in this [:vertices-to-edges] #(if (contains? % v) % (assoc % v #{}))))

  (add-hyperedge [this hedge]
    (let [hedge-set           (set hedge)
          _                   (timbre/spy hedge-set)
          new-edge-id         next-edge-id
          ;; Ensure all vertices in the new edge exist in the graph
          graph-with-vertices (reduce add-vertex this hedge-set)]
      (if (or (empty? hedge-set)
            (some #(= hedge-set %) (vals edges-to-vertices)))
        (do
          (timbre/spy "do not add...")
          this)                                             ; Do not add empty or duplicate edges
        (-> graph-with-vertices
          (assoc-in [:edges-to-vertices new-edge-id] hedge-set)
          (update :next-edge-id inc)
          (update :vertices-to-edges (fn [v-map]
                                       (reduce (fn [m v]
                                                 (timbre/spy v)
                                                 (update m v (fn [x]
                                                               (timbre/spy
                                                                 (conj x new-edge-id)))))
                                         v-map
                                         hedge-set)))))))

  (remove-vertex [this v]
    (if-not (contains? vertices-to-edges v)
      this
      (let [edges-to-update (get vertices-to-edges v)]
        (-> (reduce (fn [graph edge-id]
                      (let [updated-edge (disj (get-in graph [:edges-to-vertices edge-id]) v)]
                        (if (empty? updated-edge)
                          (update graph :edges-to-vertices dissoc edge-id)
                          (assoc-in graph [:edges-to-vertices edge-id] updated-edge))))
              this
              edges-to-update)
          (update :vertices-to-edges dissoc v)))))

  (remove-hyperedge [this hedge-id]
    (if-not (contains? edges-to-vertices hedge-id)
      this
      (let [vertices-in-edge (get edges-to-vertices hedge-id)]
        (-> this
          (update :edges-to-vertices dissoc hedge-id)
          (update :vertices-to-edges (fn [v-map]
                                       (reduce (fn [m v]
                                                 (let [updated-edges (disj (get m v) hedge-id)]
                                                   (if (empty? updated-edges)
                                                     (dissoc m v)
                                                     (assoc m v updated-edges))))
                                         v-map
                                         vertices-in-edge)))))))

  (get-vertices [this]
    (set (keys vertices-to-edges)))

  (get-hyperedges [this]
    edges-to-vertices)

  (get-edges-for-vertex [this v]
    (get vertices-to-edges v #{}))

  (get-vertices-for-edge [this hedge-id]
    (get edges-to-vertices hedge-id #{}))

  (get-neighbors [this v]
    (let [neighbor-edges (get-edges-for-vertex this v)]
      (->> neighbor-edges
        (mapcat #(get-vertices-for-edge this %))
        (into #{})
        (disj v))))                                         ; Remove the vertex itself from its neighbors

  (incident? [this v hedge-id]
    (contains? (get-vertices-for-edge this hedge-id) v)))

;; Public constructor function
(defn new-hypergraph
  "Creates a new, empty hypergraph."
  []
  (->Hypergraph {} {} 0))

(comment

  ;; Add vertices and edges
  (def g'
    (-> (new-hypergraph)
      (add-vertex :a)
      (add-hyperedge [:a :b :c])                            ; hedge-id 0
      (add-hyperedge [:c :d])                               ; hedge-id 1
      (add-hyperedge [:b :e :f])                            ; hedge-id 2
      (add-hyperedge [:b :c])))                             ; hedge-id 3
  )

(comment
  [[
    [#d2 [1 :person/name "Alice"] #d2 [1 :person/country 2]]

    #d2 [2 :country/name "USA"]
    ]
   #d2 [2 :country/continent 3]])

(comment

  (def hg
    (-> (new-hypergraph)
      (add-vertex [1 :person/name "Alice"])
      (add-vertex [1 :person/country 2])
      (add-vertex [2 :country/name "USA"])
      (add-hyperedge [[1 :person/name "Alice"]
                      [1 :person/country 2]
                      [2 :country/name "USA"]])
      (add-vertex [10 :person/name "Bob"])
      (add-vertex [10 :person/country 2])
      (add-hyperedge [[10 :person/name "Bob"]
                      [10 :person/country 2]
                      [2 :country/name "USA"]]))))

;TODO Continue here
;; can we re-use any ubergraph protocols?
;; integrate hypergraphs with zsets
;;


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
