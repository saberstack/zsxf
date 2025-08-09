(ns org.zsxf.input.datomic
  (:require [clojure.core.async :as a]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.datomic.cdc :as dd.cdc]
            [datomic.api :as dd]
            [org.zsxf.type.datomic.datom2 :as dd2]
            [org.zsxf.query :as q]
            [org.saberstack.performance.intake-monitor :as perf-intake-monitor]
            [org.zsxf.datalog.compiler :as dcc]
            [org.zsxf.type.one-item-set :as ois]
            [org.zsxf.zset :as zs]))

(defn ddatom2->zset-item [ddatom2]
  (zs/zset-item ddatom2 (zs/bool->weight (nth ddatom2 4))))

(defn tx-data->datomic-datoms2->zsets
  ;TODO check if this fn can run after disj-irrelevant-items
  ; which can speed up processing, especially initial load
  [idents-m data]
  (into []
    (comp
      (map (fn [[_e a _v _t _tf :as datom]]
             (let [a' (idents-m a)]
               (dd2/ddatom2 datom a'))))
      (map ddatom2->zset-item)
      (map ois/hash-set))
    data))

(defn zsxf-xform
  "Transformation specific to ZSXF."
  [idents-m]
  (map (fn [{:keys [data t id]}]
         {:zsets   (tx-data->datomic-datoms2->zsets idents-m data)
          :basis-t t})))

(defn init-query-with-conn
  "Initial naive implementation. Read all transactions datoms."
  [query conn]
  (let [perf-intake-monitor (perf-intake-monitor/create-monitor)]
    (dd.cdc/start-log->output!
      query
      conn
      zsxf-xform
      (fn
        ([] nil)
        ([_accum {:keys [zsets basis-t]}]
         (perf-intake-monitor/input perf-intake-monitor (count zsets))
         (q/input query zsets basis-t))
        ([final-accum] final-accum)))))


;; Tests, WIP
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn mbrainz-conn []
  (dd/connect (dd.cdc/uri-sqlite "mbrainz")))

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
  (let [conn        (mbrainz-conn)
        all-basis-t (dd.cdc/all-basis-t conn)]
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
    (dd.cdc/all-basis-t conn)))

(comment

  (def conn (mbrainz-conn))

  (= (test-query-as-of 992712) (q/get-result-as-of query 992712))

  (test-query-history conn query)

  (= (test-query-as-of 1000) (q/get-result-as-of query 1000))

  (dd/t->tx (dd/basis-t (dd/db (mbrainz-conn))))

  (q/get-result query)

  (time
    (let [conn (mbrainz-conn)]
      (dd.cdc/log->output (atom {}) conn zsxf-xform (xforms/count conj) nil nil))))
