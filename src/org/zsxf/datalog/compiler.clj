(ns org.zsxf.datalog.compiler
  (:require [pattern]))

(defn init []
  (pattern/compile-pattern '[?before 1 2 3 ?after]))

(comment
  ((init) [0 1 2 3 4]))
