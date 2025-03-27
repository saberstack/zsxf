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
  ; join c1 to c3 via ?p
  ; join [c1 c3] to c2 via ?m
  (let [datalog-query '[:find ?name
                        :where
                        [?p :person/name ?name] ;c1
                        [?m :movie/title "RoboCop"] ;c2
                        [?m :movie/director ?p] ;c3
                        ]
        index-state-all (atom {})
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
        safe-first (fn [thing]
                     (when (vector? thing)
                       (first thing)))
        safe-second (fn [thing]
                     (when (vector? thing)
                       (second thing)))
        index-state-all (atom {})
        txn-atom (atom [])
        _conn (load-learn-db txn-atom)
        xf (comp
            (xf/mapcat-zset-transaction-xf)
            (let [;; join c1 to c3 via ?m
                  pred-1 #(ds/datom-attr= % :movie/cast)
                  pred-2 #(ds/datom-attr= % :movie/director)

                  ;; join [c1 c3] to c2 via ?p
                  pred-3 #(ds/datom-attr= (safe-first %) :movie/cast)
                  pred-4 #(ds/datom-attr-val= % :person/name "Arnold Schwarzenegger")

                  ;; join [[c1 c3] c2] to c4 via ?d
                  pred-5 #(ds/datom-attr= (-> % safe-first safe-second) :movie/director)
                  pred-6 #(ds/datom-attr= % :person/name)]
              (comp
               (map (fn [zset]
                      (xf/disj-irrelevant-items
                       zset pred-1 pred-2 pred-3 pred-4 pred-5 pred-6)))
               ;; join c1 to c3 via ?m
               (xf/join-xf pred-1 ds/datom->eid pred-2 ds/datom->eid index-state-all)
               ;; join [c1 c3] to c2 via ?p
               (xf/join-xf pred-3 #(-> % safe-first (ds/datom->val))  pred-4 ds/datom->eid index-state-all)

               ;; join [[c1 c3] c2] to c4 via ?d
               (xf/join-xf pred-5 #(-> % safe-first safe-second ds/datom->val) pred-6 ds/datom->eid
                           index-state-all :last? true)
               (xforms/reduce zs/zset+))))
        query-results   (transduce
                         xf
                         zs/zset+
                         #{}
                         [@txn-atom])]
    (def chicken query-results)
    (is true)))

(comment


  ((some-fn pred-1 pred-2 pred-3 pred-4 pred-5 pred-6) matey)

  (let [])



         )


(comment (def q '[:find ?name
                :where
                [?m :movie/cast ?p] ;c1
                [?p :person/name "Arnold Schwarzenegger"] ;c2
                [?m :movie/director ?d] ;c3
                [?d :person/name ?name] ;c4
                ])


         (defn query->where-clauses [q]
           (->> q
                (drop-while #(not= :where %))
                (drop  1)))

         (defn all-where-variables [q]
           (->> q
                query->where-clauses
                (mapcat identity)
                (filter symbol?)
                set))
         (all-where-variables q)

         (def where-clauses (query->where-clauses q))

         (set! *print-meta* false)

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
                   (update e (fnil assoc {}) clause-name :entity)
                   (parser/variable? v)
                   (update v (fnil assoc {}) clause-name :value)))
            {}
            named-clauses))
         (index-variables (name-clauses where-clauses))


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
         (def adjacency-list (build-adjacency-list (name-clauses where-clauses)))

         (defn clause-pred [[e a v]]
           (if (parser/variable? v)
             #(ds/datom-attr= % a)
             #(ds/datom-attr-val= % a v)))
         (def pos->getter
           {:entity ds/datom->eid
            :value ds/datom->val})

         (defn* where-xf [where-clauses]
           (let [named-clauses (name-clauses where-clauses)
                 adjacency-list (build-adjacency-list named-clauses)
                 adjacency-tuples (for [[from-node destinations] adjacency-list
                                        [to-node _] destinations];
                                    [from-node to-node])
                 [first-clause & remaining-clauses] (keys named-clauses)
                 join-order (loop [join-order [first-clause]
                                   remaining-nodes (set remaining-clauses)
                                   n 1]

                              (cond (empty? remaining-nodes)
                                    join-order

                                    (> n 15)
                                    :oops

                                    :else
                                    (let [covered-nodes (set join-order)
                                          [_ to] (medley/find-first
                                                  (fn [[from to]]

                                                    (and (covered-nodes from)
                                                         (remaining-nodes to)))
                                                  adjacency-tuples)]
                                      (recur
                                       (conj join-order to)
                                       (disj remaining-nodes to)
                                       (inc n)))))]
             (loop
                 [xf-steps []
                  lh-nodes (take 1 join-order)
                  next-join (take 2 join-order)
                  remaining (drop 2 join-order)
                  n 1]
               (cond
                 (empty? remaining)
                 xf-steps

                 (> n 1)
                 {:overflow xf-steps}

                 :else
                 (let [[lhs rhs] next-join
                       [c1 c2] (map named-clauses [lhs rhs])
                       common-var (get-in adjacency-list [lhs rhs])
                       left-pos (get-in variable-index [common-var lhs])
                       right-pos (get-in variable-index [common-var rhs])
                       new-join [:join (clause-pred c1) (pos->getter left-pos) (clause-pred c2) (pos->getter right-pos)]]
                   (recur
                    (conj xf-steps new-join)
                    (conj covered lhs rhs)
                    [[lhs rhs] (first remaining)]
                    (rest remaining)
                    (inc 1)
                    )
                   ))

               )))
         adjacency-list
         (where-xf where-clauses)


; so the pred is always datom-attr= or datom-attr-val=
; the attr is the attr from the datom
; if the val is needed it's the val from the datom
; then you have this getter as the first arg to the pred
; it's a datom selector: for first level it's just %
; after that you are finding the datom through combinations of first and second
;
;ok but then we have join key
;it's really just that pred arg composed with the getter
;
;so really, this is simple
;
;for each var represented in the join, track a selector
;...to be continued
;
         )


#_(deftest defn-test
  (is (= true
         (subject/foo))))
#_(load-learn-db)
; take all where clauses
;
; give each a name
;
; make an adjacency list graph of sorts: for each clause, say which other clauses it points to
; and/or to invert this: for each variable, note the clauses in which it is found
;
; The nodes/vertices in the graph are clauses, and the edges are shared variables
;
; it's not even graph traversal, because we don't need to go in order
; we don't need a vertex cover either - we do need to use every edge, but actually at the end we will just use up the leftovers
;
; Traversing an edge means writing a join along that variable between the two nodes
;
; I think it's edge cover: we need a set of joins/edges that touches every clause
;
; I'm not sure if these are necessarily connected or not - probably not
;
; Also, we only want to do joins based on [E linking E] or [V linking E], with [V linking V] as a last resort.
; I think we can implement this by weighing the "good" kinds of edges at 0 and the [V linking V] edges at 1, and using a minimum edge cover algorithm
;
; Maybe I shouldn't worry about the proper graph term so much, because our problem space is pretty constrained:
;
; - there is a max of two edges per vertex
; - we have to pick at least one edge per vertex
;
; There will be a single connected acyclic component here, because we have to join to what is in the query so far. It's not strict traversal though because if we first go A->B, we don't have to go from B next, but we could also go A->C.
;
; I'm thinking an algorithm like:
;  - start on the node that has the variable in the find clause (assuming just one for first iteration, even though it's not a good assumption)
;  - pick one of its edges (of minimum cost) A->B
;  - now you can choose between the other A edge, or the other B edge. Pick one (minimum cost) that brings in a new node
;    * cycles won't happen because we are always bringing in a new node
;  - repeat until every vertex is covered
;
; then with the leftover edges, we have to filter
