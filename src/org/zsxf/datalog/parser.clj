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
