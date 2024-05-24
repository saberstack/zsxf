(ns org.zsxf.experimental.core
  (:require [org.zsxf.experimental.dataflow :as xp-dataflow]
            [org.zsxf.experimental.demo-1 :as xp-demo]))


;A SQL query
(defn compile-query
  ""
  [^String query]
  (comment
    ;; implementation pending
    (compile! query))
  )

;How to use:
(comment
  ; 1. compile a query to an incremental dataflow graph;
  (compile-query
    "SELECT * FROM zsxf.team t
     JOIN zsxf.player p ON t.id = p.team
     WHERE t.id = 20")

  ;incremental computation example
  (xp-dataflow/incremental-computation-xf)

  ;; ... library

  ; 2. view the results
  (xp-demo/final-result)
  )
