(ns org.zsxf.experimental.query-validation
  (:require [clojure.spec.alpha :as s]))

(s/def ::datalog-query vector?)

(defmacro invalid-query [datalog-query]
  (when-not (s/valid? ::datalog-query datalog-query)
    (throw (ex-info "We don't support that yet"
             {:datalog-query datalog-query
              :spec-said     (s/explain-data ::datalog-query datalog-query)}))))

(comment
  (invalid-query
    {:find  '[?a]
     :where []})
  ;=>
  ; Syntax error macroexpanding invalid-query at ...
  ; We don't support that yet
  ;
  *e
  ;=>
  ; full exception with the query, explain data, etc.
  )
