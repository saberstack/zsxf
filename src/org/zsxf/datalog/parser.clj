(ns org.zsxf.datalog.parser
  (:require [clojure.string :as string])
  )

(defn variable?
  "Returns true if x is a variable per https://docs.datomic.com/query/query-data-reference.html#variables
   False otherwise."
  [x]
  (and (symbol? x) (string/starts-with? (str x) "?")))
