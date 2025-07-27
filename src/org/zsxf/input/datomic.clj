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
      (map ddatom2->zset-item)
      (map hash-set))
    data))

(defn zsxf-xform
  "Transformation specific to ZSXF."
  [idents-m]
  (map (fn [{:keys [data t id]}]
         {:zsets (tx-data->datomic-datoms2->zsets idents-m data)
          :basis-t         t})))

(defn init-query-with-conn
  "Initial naive implementation. Read all transactions datoms."
  [query conn]
  (dcdc/start-log->output!
    query
    conn
    zsxf-xform
    (completing
      ;accum is not used, this is a side-effecting reducing fn
      (fn
        [_accum {:keys [zsets basis-t]}]
        (q/input query zsets :basis-t basis-t)))))


;; Tests, WIP
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn mbrainz-conn []
  (dd/connect (dcdc/uri-sqlite "mbrainz")))

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
    (init-query-with-conn query (mbrainz-conn))))

(defn test-query-as-of [basis-t]
  (let [conn (mbrainz-conn)
        all-basis-t (dcdc/all-basis-t conn)]
    (dd/q
      '[:find ?artist-name
        :where
        [?c :country/name-alpha-2 "BG"]
        [?a :artist/country ?c]
        [?a :artist/name ?artist-name]]
      (dd/as-of (dd/db conn) basis-t))))

(defn test-query-history [conn query]
  (transduce
    (map (fn [basis-t]
           (= (test-query-as-of basis-t) (q/get-result-as-of query basis-t))))
    conj
    (dcdc/all-basis-t conn)))

(comment

  (def conn (mbrainz-conn))

  (first (dcdc/all-basis-t conn))

  (= (test-query-as-of 992712) (q/get-result-as-of query 992712))

  (test-query-history conn query)

  (= (test-query-as-of 1000) (q/get-result-as-of query 1000))

  (dd/t->tx (dd/basis-t (dd/db (mbrainz-conn))))

  (q/get-result query)

  (time
    (let [conn (mbrainz-conn)]
      (dcdc/log->output (atom {}) conn zsxf-xform (xforms/count conj)))))
