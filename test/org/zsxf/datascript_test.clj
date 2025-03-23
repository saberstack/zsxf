(ns org.zsxf.datascript-test
  (:require
   [clojure.core.async :as a]
   [clojure.test :refer [deftest is]]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [datascript.core :as d]
   [org.zsxf.datascript :as ds]
   [org.zsxf.zset :as zs]
   [org.zsxf.xf :as xf]
   [org.zsxf.experimental.datastream :as data-stream]
   [net.cgrand.xforms :as xforms]
   [taoensso.timbre :as timbre])
  (:import
   [java.io PushbackReader]))


(defn load-edn-file [file-path]
  (with-open [rdr (io/reader file-path)]
    (edn/read (PushbackReader. rdr))))

(defn load-learn-db
  ([]
   (load-learn-db nil)
   )
  ([listen-atom]
   (let [schema (load-edn-file "resources/learndatalogtoday/schema_datascript.edn")
         data (load-edn-file   "resources/learndatalogtoday/data_datascript.edn")
         conn (d/create-conn schema)]
     (when listen-atom
       (data-stream/listen-datom-stream conn listen-atom ds/tx-datoms->zset))
     (d/transact! conn data)
     conn)))

(defonce result-set (atom #{}))

(deftest test-robocop "basic datalog query"
  (let [input (a/chan)
        txn-atom (atom [])
        index-state-all (atom {})
        conn (load-learn-db txn-atom)
        xf (comp
            (xf/mapcat-zset-transaction-xf)
            (let [pred-1 #(ds/datom-attr= % :person/name)
                  pred-2 #(ds/datom-attr= % :movie/director)
                  pred-3 #(ds/datom-attr= (second %) :movie/director)
                  pred-4 #(ds/datom-attr-val= % :movie/title "RoboCop")]
              (comp
               (map (fn [zset]
                      (xf/disj-irrelevant-items
                       zset pred-1 pred-2 pred-3 pred-4)))
               (map (fn [tx-current-item] (timbre/spy tx-current-item)))
               (xf/join-xf
                pred-1 ds/datom->eid
                pred-2 ds/datom->val
                index-state-all)
               (map (fn [zset-in-between] (timbre/spy zset-in-between)))
               (xf/join-xf
                pred-3 #(-> % second (ds/datom->eid))
                pred-4 ds/datom->eid
                index-state-all
                :last? true)
               (map (fn [zset-in-between-last] (timbre/spy zset-in-between-last)))
               (xforms/reduce zs/zset+))))
          output-ch (a/chan (a/sliding-buffer 1)
                      (xf/query-result-set-xf result-set))
          _        (a/pipeline 1 output-ch xf input)]
    (a/>!! input @txn-atom)
    (timbre/info "done with tx"))
  (is true))

(deftest test-robocop-transduce "basic datalog query"
  (let [index-state-all (atom {})
        txn-atom (atom [])
        _conn (load-learn-db txn-atom)
        xf (comp
             (xf/mapcat-zset-transaction-xf)
             (let [pred-1 #(ds/datom-attr= % :person/name)
                   pred-2 #(ds/datom-attr= % :movie/director)
                   pred-3 #(ds/datom-attr= (second %) :movie/director)
                   pred-4 #(ds/datom-attr-val= % :movie/title "RoboCop")]
               (comp
                 (map (fn [zset]
                        (xf/disj-irrelevant-items
                          zset pred-1 pred-2 pred-3 pred-4)))
                 (map (fn [tx-current-item] (timbre/spy tx-current-item)))
                 (xf/join-xf
                   pred-1 ds/datom->eid
                   pred-2 ds/datom->val
                   index-state-all)
                 (map (fn [zset-in-between] (timbre/spy zset-in-between)))
                 (xf/join-xf
                   pred-3 #(-> % second (ds/datom->eid))
                   pred-4 ds/datom->eid
                   index-state-all
                   :last? true)
                 (map (fn [zset-in-between-last] (timbre/spy zset-in-between-last)))
                 (xforms/reduce zs/zset+))))]
    (is
      (=
        (transduce
          xf
          zs/zset+
          #{}
          [@txn-atom])
        #{^#:zset{:w 1}
          [^#:zset{:w 1} [^#:zset{:w 1} [16 :person/name "Paul Verhoeven"]
                          ^#:zset{:w 1} [59 :movie/director 16]]
           ^#:zset{:w 1} [59 :movie/title "RoboCop"]]}))))

(comment [:find ?name
   :where
   [?p :person/name ?name] ;c1
   [?m :movie/title "RoboCop"] ;c2
   [?m :movie/director ?p] ;c3
   ]

;c1 unifies to c3 by ?p
;c2 unifies to c3 by ?p

;so that means we are joining c1 to c3 and c2 to c3?2
         )


#_(deftest defn-test
  (is (= true
         (subject/foo))))
#_(load-learn-db)
