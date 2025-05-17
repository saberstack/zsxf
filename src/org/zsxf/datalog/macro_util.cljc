(ns org.zsxf.datalog.macro-util
  (:require [org.zsxf.datom :as d2]))

(defn safe-first [thing]
  (when (vector? thing)
    (first thing)))

(defn safe-second [thing]
  (when (vector? thing)
    (second thing)))
