(ns org.zsxf.input.datomic
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.datomic.cdc :as dcdc]
            [datomic.api :as dd]
            [org.zsxf.type.datomic.datom2 :as dd2]
            [org.zsxf.query :as q]
            [org.zsxf.datalog.compiler :as dcc]
            [org.zsxf.zset :as zs]))

(defn ddatom2->zset-item [ddatom2]
  (zs/zset-item ddatom2 (zs/bool->weight (nth ddatom2 4))))

(defn tx-data->datomic-datoms2->zsets
  [idents-m data]
  (into []
    (comp
      (map (fn [[_e a _v _t _tf :as datom]]
             (let [a' (get idents-m a)]
               (dd2/ddatom2 datom a'))))
      (map (fn [ddatom2] ddatom2))
      (map ddatom2->zset-item)
      (map hash-set))
    data))

(defn ->zsxf-xf
  "Transformation specific to ZSXF"
  [idents-m]
  (map (fn [{:keys [data t id]}]
         (tx-data->datomic-datoms2->zsets idents-m data))))

(defn init-query-with-conn
  "Initial naive implementation. Read all transactions datoms."
  [query conn]
  (dcdc/log->output!
    (q/get-id query)
    conn
    (completing
      (fn
        ([] :todo)
        ([_accum datoms2]
         (q/input query datoms2))))
    ->zsxf-xf))

(defn sample-conn []
  (dd/connect (dcdc/db-uri-sqlite "mbrainz")))

(defn poc-query []
  (let [query (q/create-query
                (dcc/static-compile
                  '[:find ?artist-name
                    :where
                    [?c :country/name-alpha-2 "BG"]
                    [?a :artist/country ?c]
                    [?a :artist/name ?artist-name]]))]
    (def query query)
    ;init
    (init-query-with-conn query (sample-conn))))

(comment

  (sample-conn)

  (dd/t->tx (dd/basis-t (dd/db (sample-conn))))

  (q/get-result query)

  (time
    (let [conn (sample-conn)]
      (dcdc/log->output (atom {}) conn (xforms/count conj) ->zsxf-xf))))
