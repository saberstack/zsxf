(ns org.zsxf.input.datomic
  (:require [org.zsxf.datomic.cdc :as dcdc]
            [org.zsxf.query :as q]))

(defn ->zsxf-xf
  "Transformation specific to ZSXF"
  [idents-m]
  (comp
    (map (fn [{:keys [data t id]}]
           (dcdc/tx-data->datoms idents-m data)))
    (map (fn [datoms]
           (q/input )))))

(defn init-query-with-conn
  "Initial naive implementation. No listeners or change data capture.
  Read all transactions datoms."
  [query conn]
  ;TODO WIP
  (dcdc/datomic-tx-log->output conn conj ->zsxf-xf)
  )
