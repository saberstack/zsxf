(ns org.zsxf.datalog.parser
  (:require [clojure.set :as set]
            [taoensso.timbre :as timbre]
            [medley.core :as medley])
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
  (transduce
    (map identity)
    (completing
      (fn [accum clause]
        (let [[e _a _v] clause
              ;TODO make intention behind conj? more clear
              [ref-found? clause]
              (if (variable? e)
                (let [result (medley/find-first (fn [[_e _a v]] (= e v)) clauses)]
                  (if result
                    [true (conj (pop result) clause)]      ;return
                    [false clause]))                         ;return
                [false clause])]                             ;return
          (if ref-found?
            (conj accum clause)
            accum))))
    []
    clauses))

; Usage example
(comment
  (where-clauses-to-graph
    '[[?team-eid :team/name ?team-name]
      [?player-eid :player/team ?team-eid]
      [?player-eid :player/name ?player-name]
      ]))
