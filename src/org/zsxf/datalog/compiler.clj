(ns org.zsxf.datalog.compiler
  (:require [pattern :as p]))

(defn init []
  (p/compile-pattern '[?before 1 2 3 ?after]))

(comment
  ((init) [0 1 2 3 4]))

(def m (p/compile-pattern '[?fn-name-1 (fn ?fn-name-2 [??args] ??body)]))

(m ['abc '(fn abc [x y z] x)])                              ; => {:fn-name 'abc ...}
(m ['xyz '(fn abc [x] x)])                                  ; => nil

(def example-datalog-query-1
  '[:find (pull ?exercise-set-eids [:db/id])
    :in $ ?workout-uuid
    :where
    [?workout-eid :workout/uuid ?workout-uuid]
    [?exercise-set-eids :exercise-set/workout ?workout-eid]])


(def datalog->zsxf-matcher
  (p/compile-pattern
    '[??_
      :where ??clauses]))

(defn datalog->zsxf [query]
  (datalog->zsxf-matcher query))

(comment
  (datalog->zsxf example-datalog-query-1))
