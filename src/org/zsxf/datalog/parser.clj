(ns org.zsxf.datalog.parser
  (:require [clojure.set :as set])
  (:import (clojure.lang IPersistentVector)))


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

(defn where-clauses-to-graph [clauses]
  ;TODO WIP
  (transduce
    (comp
      (map identity))
    conj
    []
    clauses))

; Usage example
(comment
  (where-clauses-to-graph
    '[[?team-eid :team/name ?team-name]
      [?player-eid :player/team ?team-eid]
      [?player-eid :player/name ?player-name]]))
