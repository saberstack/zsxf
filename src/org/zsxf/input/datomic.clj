(ns org.zsxf.input.datomic
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.datomic.cdc :as dcdc]
            [datomic.api :as dd]
            [org.zsxf.type.datomic.datom2 :as dd2]
            [org.zsxf.query :as q]
            [taoensso.timbre :as timbre]))

(defn tx-data->datomic-datoms2
  [idents-m data]
  (into []
    (map (fn [[_e a _v _t _tf :as datom]]
           (let [a' (get idents-m a)]
             ;[e a' v t tf]
             (dd2/ddatom2 datom a'))))
    data))

(defn ->zsxf-xf
  "Transformation specific to ZSXF"
  [idents-m]
  (comp
    (map (fn [{:keys [data t id]}]
              (tx-data->datomic-datoms2 idents-m data)))))

(defn init-query-with-conn
  "Initial naive implementation. No listeners or change data capture.
  Read all transactions datoms."
  [query conn]
  (dcdc/datomic-tx-log->output conn
    (completing
      (fn
        ([] :todo)
        ([_accum datoms2]
         ;TODO WIP
         (timbre/info "Processing datoms2" (count datoms2))
         (q/input query datoms2))))
    ->zsxf-xf))

(defn sample-conn []
  (dd/connect (dcdc/db-uri "mbrainz")))

(comment

  (init-query-with-conn nil (sample-conn))

  (let [conn (sample-conn)]
    (def tmp
      (last
        (take
          50
          (dcdc/datomic-tx-log->output conn conj ->zsxf-xf)))))

  (let [conn (sample-conn)]
    (reset! dcdc/cdc-ch (a/chan 10000))
    (dcdc/datomic-tx-log->output conn (dcdc/->reduce-to-chan @dcdc/cdc-ch) ->zsxf-xf))

  (time
    (let [conn (sample-conn)]
      (reset! dcdc/cdc-ch (a/chan 10000))
      (dcdc/datomic-tx-log->output conn (xforms/count conj) ->zsxf-xf))))
