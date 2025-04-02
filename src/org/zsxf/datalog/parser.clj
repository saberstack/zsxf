(ns org.zsxf.datalog.parser
  (:require [clojure.string :as string])
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
