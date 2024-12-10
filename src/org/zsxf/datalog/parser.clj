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

(defn nest-clauses [ref-clause clause]
  (conj (pop ref-clause) clause))

(defn where-clauses-to-nested-clauses
  ([^IPersistentVector clause ^IPersistentSet all-clauses]
   (where-clauses-to-nested-clauses clause all-clauses #{}))
  ([^IPersistentVector clause ^IPersistentSet all-clauses ^IPersistentSet used-clauses]
   (let [[e _a _v :as clause] clause]
     (if (variable? e)
       (let [ref-clause (find-ref-clause e all-clauses)]
         (if ref-clause
           ;recursion
           (trampoline where-clauses-to-nested-clauses (nest-clauses ref-clause clause) all-clauses (conj used-clauses ref-clause))
           ;return
           {:clause clause :used-clauses used-clauses}))
       ;no variable found
       {:clause clause :used-clauses used-clauses}))))

; Usage example
(comment
  (where-clauses-to-nested-clauses
    '[?team-eid :team/name ?team-name]
    '[[?team-eid :team/name ?team-name]
      [?player-eid :player/team ?team-eid]
      [?player-eid :player/name ?player-name]
      [?z :a/b ?player-eid]
      ]))

(defn where-clauses-to-graph [clauses]
  (let [all-clauses (set clauses)]
    (transduce
      (map (fn [clause] (where-clauses-to-nested-clauses clause all-clauses)))
      (completing
        (fn [accum {:keys [clause used-clauses] :as _item}]
          (let [prev-used-clauses (get accum :used-clauses)
                next-used-clauses (clojure.set/union prev-used-clauses used-clauses)]
            (-> accum
              (update :graph (fn [graph] (apply disj (conj graph clause) next-used-clauses)))
              (assoc :used-clauses next-used-clauses)))))
      {:graph        #{}
       :used-clauses #{}}
      clauses)))

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
