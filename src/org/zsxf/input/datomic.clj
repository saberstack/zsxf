(ns org.zsxf.input.datomic
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.datomic.cdc :as dcdc]
            [datomic.api :as dd]
            [org.zsxf.type.datomic.datom2 :as dd2]
            [org.zsxf.query :as q]
            [org.zsxf.datalog.compiler :as dcc]
            [org.zsxf.zset :as zs]
            [taoensso.timbre :as timbre]))

(defn ddatom2->zset-item [ddatom2]
  (zs/zset-item ddatom2 (zs/bool->weight (nth ddatom2 4))))

(defn tx-data->datomic-datoms2->zsets
  [idents-m data]
  (into []
    (comp
      (map (fn [[_e a _v _t _tf :as datom]]
             (let [a' (get idents-m a)]
               (dd2/ddatom2 datom a'))))
      (map ddatom2->zset-item)
      (map hash-set))
    data))

(defn ->zsxf-xf
  "Transformation specific to ZSXF"
  [idents-m]
  (comp
    (map (fn [{:keys [data t id]}]
           (tx-data->datomic-datoms2->zsets idents-m data)))))

(defonce tmp (atom nil))

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
         (reset! tmp datoms2)
         (timbre/info "Processing datoms2" (count datoms2))
         (q/input query datoms2))))
    ->zsxf-xf))

(defn sample-conn []
  (dd/connect (dcdc/db-uri "mbrainz")))

(defn poc-query []
  (let [query (q/create-query
                (dcc/static-compile
                  '[:find ?name
                    :where
                    [?c :country/name-alpha-2 ?name]]))]
    ;init
    (init-query-with-conn query (sample-conn))
    (q/get-result query)))1

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
