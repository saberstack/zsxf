(ns org.zsxf.datalog.parser
  (:require [clojure.set :as set]
            [taoensso.timbre :as timbre]
            [medley.core :as medley])
  (:import (clojure.lang IPersistentSet IPersistentVector)))


(defn where-clauses-independent? [clauses]
  (transduce
    (comp
      (map (fn [^IPersistentVector clause]
             (filterv symbol? clause)))                     ; remove non-symbols from each clause
      (map (fn [clause]
             (into #{} clause))))                           ; turn each clause into a set
    (completing
      conj
      (fn [v]
        (let [common-syms (apply set/intersection v)]       ; find the intersection of all the sets (if any)
          (empty? common-syms))))
    []
    clauses))


(comment
  (where-clauses-independent?
    '[[?e :name ?v]
      [?e2 :name ?v2]]))

(defn variable?
  "Returns true if x is a variable per https://docs.datomic.com/query/query-data-reference.html#variables
   False otherwise."
  [x]
  (and (symbol? x) (clojure.string/starts-with? (str x) "?")))

(defn find-ref-clause
  "Returns a clause or nil if not found"
  [variable clauses]
  (medley/find-first (fn [[_e _a v]] (= variable v)) clauses))

(defn- where-clauses-to-graph-impl [^IPersistentVector dag ^IPersistentSet clauses]
  (let [[e _a _v :as clause] (first clauses)]
    (if clause
      ;clause found, build DAG
      (if (variable? e)
        (let [ref-clause (find-ref-clause e clauses)
              dag'       (conj dag
                           (if ref-clause
                             (conj (pop ref-clause) clause)
                             clause))]
          (where-clauses-to-graph-impl dag' (disj clauses ref-clause clause)))
        ;no variable found
        (where-clauses-to-graph-impl (conj dag clause) (disj clauses clause)))
      ;stop recursion and return
      dag)))

(defn where-clauses-to-graph [clauses]
  (where-clauses-to-graph-impl [] (timbre/spy (set clauses))))

; Usage example
(comment
  (where-clauses-to-graph
    '[[?team-eid :team/name ?team-name]
      [?player-eid :player/team ?team-eid]
      [?player-eid :player/name ?player-name]
      ])

  (where-clauses-to-graph
    '[[?team-eid :team/name ?team-name]
      [?player-eid :player/team ?team-eid]

      [?player-eid :player/team ?team-eid2]
      [?team-eid2 :team/name ?team-name]

      ]))
