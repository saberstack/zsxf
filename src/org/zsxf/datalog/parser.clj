(ns org.zsxf.datalog.parser
  (:require [clojure.string :as string]
            [clojure.set :as set])

  )

(defn variable?
  "Returns true if x is a variable per https://docs.datomic.com/query/query-data-reference.html#variables
   False otherwise."
  [x]
  (and (symbol? x) (string/starts-with? (str x) "?")))

(defn query->map [q]
  (second (reduce
           (fn [[current-segment acc] el]
                (if (keyword? el)
                  [el acc]
                  [current-segment (update acc current-segment (fnil conj []) el)]))
           [nil {}]
           q)))


(defn name-clauses
  "Takes the where clauses of a datalog query and gives each a name, e.g. :c1.
  Returns a map from name to clause."
  [where-clauses]
  (into {}
        (map-indexed (fn [idx el]
                       [(keyword (format "c%s" (inc idx))) el]))
        where-clauses))

(defn index-variables
  "Builds a map from each variable appearing in the where clauses
  to the clauses they appear in, and in which position.
  Returns map from variable to map from clause name to positional getter function."
  [named-clauses]
  (reduce
   (fn [acc [clause-name [e _ v]]]
     (cond-> acc
       (variable? e)
       (update e (fnil assoc {}) clause-name :entity)
       (variable? v)
       (update v (fnil assoc {}) clause-name :value)))
   {}
   named-clauses))

(defn build-adjacency-list
  "Where clauses in a datalog query can be modeled as the nodes
  of a graph. The edges are common variables between clauses.
  Returns a map from source clause name to a map from destination
  clause name to the common variable (edge)."
  [named-clauses]
  (let [variable-index (index-variables named-clauses)]
    (reduce
     (fn [acc [clause-name [e _ v]]]
       (assoc acc clause-name
              (into {} (concat
                        (when (variable? e)
                          (for [dest (->> variable-index e keys (filter (partial not= clause-name)))]
                            [dest e]))

                        (when (variable? v)
                          (for [dest (->> variable-index v keys (filter (partial not= clause-name)))]
                            [dest v]))))))
     {}
     named-clauses)))

(defn connected-to "In a graph (represented as adjacency-list), recursively finds
  all of the nodes that the given node is connected to, excluding those which are already-connected.

  Returns a set of nodes."
  [adjacency-list already-connected clause]
  (let [degree-one (apply sorted-set (keys (get adjacency-list clause)))
        next-nodes (set/difference degree-one already-connected)]
    (if (empty? next-nodes)
      (sorted-set clause)
      (let [now-connected (set/union already-connected next-nodes)
            recur-on-next (partial connected-to adjacency-list now-connected)
            sets-of-nodes (concat [(sorted-set clause) next-nodes] (map recur-on-next next-nodes))]
        (reduce set/union sets-of-nodes)))))

(defn find-connected-components
  "Given a graph, represented as an adjacency list, returns a vector of
  sets of connected components within that graph."
  [adjacency-list]
  (let [[first-clause & _ :as all-clauses] (apply sorted-set (keys adjacency-list))
         first-component (connected-to adjacency-list (sorted-set first-clause) first-clause)]
        (loop [components [first-component]
           covered first-component
           [next-clause & _ :as  remaining] (clojure.set/difference all-clauses first-component)]
      (if (empty? remaining)
        components
        (let [next-component (connected-to adjacency-list covered next-clause)]
          (recur
           (conj components next-component)
           (clojure.set/intersection covered next-component)
           (clojure.set/difference remaining next-component)))))))
