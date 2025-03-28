(ns org.zsxf.datascript-test
  (:require
   [clojure.core.async :as a]
   [clojure.test :refer [deftest is]]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [datascript.core :as d]
   [medley.core :as medley]
   [org.zsxf.datascript :as ds]
   [org.zsxf.datalog.parser :as parser]
   [org.zsxf.zset :as zs]
   [org.zsxf.xf :as xf]
   [org.zsxf.experimental.datastream :as data-stream]
   [net.cgrand.xforms :as xforms]
   [org.zsxf.util :as util :refer [nth2]]
   [taoensso.timbre :as timbre]
   [clojure.set :as set])
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

(defn query->where-clauses [q]
  (->> q
       (drop-while #(not= :where %))
       (drop  1)))

(defn name-clauses [where-clauses]
  (second
   (reduce
    (fn [[n acc] clause]
      [(inc n) (assoc acc (keyword (format "c%s" n)) clause)])
    [1 {}]
    where-clauses)))

(defn index-variables [named-clauses]
  (reduce
   (fn [acc [clause-name [e _ v]]]
     (cond-> acc
       (parser/variable? e)
       (update e (fnil assoc {}) clause-name ds/datom->eid )
       (parser/variable? v)
       (update v (fnil assoc {}) clause-name ds/datom->val)))
   {}
   named-clauses))

(defn build-adjacency-list [named-clauses]
  (let [variable-index (index-variables named-clauses)]
    (reduce
     (fn [acc [clause-name [e _ v]]]
       (assoc acc clause-name
              (into {} (concat
                        (when (parser/variable? e)
                          (for [dest (->> variable-index e keys (filter (partial not= clause-name)))]
                            [dest e]))

                        (when (parser/variable? v)
                          (for [dest (->> variable-index v keys (filter (partial not= clause-name)))]
                            [dest v]))))))
     {}
     named-clauses)))

(defn safe-first [thing]
  (when (vector? thing)
    (first thing)))

(defn safe-second [thing]
  (when (vector? thing)
    (second thing)))

(defn clause-pred [locator [e a v]]
  (if (parser/variable? v)
    #(ds/datom-attr= (locator %) a)
    #(ds/datom-attr-val= (locator %) a v)))
(def pos->getter
  {:entity ds/datom->eid
   :value ds/datom->val})

(defn where-xf [datalog-query state]
  (let [where-clauses (query->where-clauses datalog-query)
        named-clauses (name-clauses where-clauses)
        variable-index (index-variables named-clauses)
        adjacency-list (build-adjacency-list named-clauses)
        adjacency-tuples (for [[from-node destinations] adjacency-list
                               [to-node _] destinations];
                           [from-node to-node])
        [first-clause & remaining-clauses] (keys named-clauses)]
    (loop [preds []
           xf-steps []
           covered-nodes #{first-clause}
           remaining-nodes (set remaining-clauses)
           locators {first-clause identity}
           n 1]

      (cond (empty? remaining-nodes)
            (apply comp (concat
                         [(xf/mapcat-zset-transaction-xf)
                          (fn [zset]
                            (apply xf/disj-irrelevant-items
                                   (cons zset preds)))]
                         xf-steps
                         [(xforms/reduce zs/zset+)]))

            (> n 10)
            n

            :else
            (let [[from to :as edge] (medley/find-first
                                      (fn [[from to]]
                                        (and (covered-nodes from)
                                             (remaining-nodes to)))
                                      adjacency-tuples)
                  common-var (get-in adjacency-list [from to])
                  [c1 c2] (map named-clauses edge)
                  [p1 p2] [(clause-pred (from locators) c1) (clause-pred identity c2)]
                  new-join (xf/join-xf p1
                                       (comp (get-in variable-index [common-var from]) (from locators))
                                       p2
                                       (get-in variable-index [common-var to])
                                       state
                                       :last? (= #{to} remaining-nodes))]
              (recur (conj preds p1 p2)
                     (conj xf-steps new-join)
                     (conj covered-nodes to)
                     (disj remaining-nodes to)
                     (-> (medley/map-vals #(comp % safe-first) locators)
                         (assoc to safe-second)
                         )
                     (inc n)))))))

(deftest test-robocop-transduce "basic datalog query"
  (let [datalog-query '[:find ?name
                        :where
                        [?p :person/name ?name]
                        [?m :movie/title "RoboCop"]
                        [?m :movie/director ?p]
                        ]
        index-state-all (atom {})
        txn-atom (atom [])
        _conn (load-learn-db txn-atom)
        xf (where-xf datalog-query index-state-all)]
    (is
     (=
      (transduce
       xf
       zs/zset+
       #{}
       [@txn-atom])
      #{^#:zset{:w 1}
        [[[16 :person/name "Paul Verhoeven"]
          [59 :movie/director 16]]
         [59 :movie/title "RoboCop"]]}))))

(deftest test-ahhnold "Another basic query"
  (let [query '[:find ?name
                :where
                [?m :movie/cast ?p] ;c1
                [?p :person/name "Arnold Schwarzenegger"] ;c2
                [?m :movie/director ?d] ;c3
                [?d :person/name ?name] ;c4
                ]

        index-state-all (atom {})
        txn-atom (atom [])
        _conn (load-learn-db txn-atom)
        xf (where-xf query index-state-all)
        query-results (transduce
                       xf
                       zs/zset+
                       #{}
                       [@txn-atom])]
    (def chicken query-results)
    (is true)))

(comment (def q '[:find ?name
                  :where
                  [?m :movie/cast ?p] ;c1
                  [?p :person/name "Arnold Schwarzenegger"] ;c2
                  [?m :movie/director ?d] ;c3
                  [?d :person/name ?name] ;c4
                  ])

         (def where-clauses (query->where-clauses q))

         (set! *print-meta* false)

         (def adjacency-list (build-adjacency-list (name-clauses where-clauses)))


         (where-xf where-clauses)

         )
