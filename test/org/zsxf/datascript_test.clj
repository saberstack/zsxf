(ns org.zsxf.datascript-test
  (:require
   [clojure.core.async :as a]
   [clojure.test :refer [deftest is]]
   [clojure.set :as set]
   [datascript.core :as d]
   [medley.core :as medley]
   [org.zsxf.datascript :as ds]
   [org.zsxf.datalog.parser :as parser]
   [org.zsxf.query :as q]
   [org.zsxf.zset :as zs]
   [org.zsxf.xf :as xf]
   [org.zsxf.experimental.datastream :as data-stream]
   [net.cgrand.xforms :as xforms]
   [org.zsxf.util :as util :refer [nth2]]
   [taoensso.timbre :as timbre]
   [clojure.set :as set]))

(defn load-learn-db
  ([]
   (load-learn-db nil)
   )
  ([listen-atom]
   (let [schema (util/load-edn-file "resources/learndatalogtoday/schema_datascript.edn")
         data (util/load-edn-file   "resources/learndatalogtoday/data_datascript.edn")
         conn (d/create-conn schema)]
     (when listen-atom
       (data-stream/listen-datom-stream conn listen-atom ds/tx-datoms->zset))
     (d/transact! conn data)
     conn)))

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

(defmacro clause-pred-macro [locator-vec [e a v :as clause]]
  (timbre/spy locator-vec)
  (timbre/spy clause)
  (if (parser/variable? v)
    `#(ds/datom-attr=  ((comp ~@locator-vec) %) ~a)
    `#(ds/datom-attr-val= ((comp ~@locator-vec) %) ~a ~v)))
                                        ;(clause-pred-macro [safe-first safe-second identity] [?m :movie/cast ?p])
(defn safe-second [thing]
  (when (vector? thing)
    (second thing)))

(defn clause-pred [locator [e a v]]
  (if (parser/variable? v)
    #(ds/datom-attr= (locator %) a)
    #(ds/datom-attr-val= (locator %) a v)))


(defn where-xf [datalog-query]
  (fn [state]
    (let [where-clauses (query->where-clauses datalog-query)
          ;; Give each clause a shorthand name, eg :c1
          named-clauses (name-clauses where-clauses)
          ;; For each variable, tell me which clauses it appears in, and in which position.
          variable-index (index-variables named-clauses)
          ;; For each clause, tell me the other clauses it leads to, and by what variable.
          adjacency-list (build-adjacency-list named-clauses)
          ;; Process that to just a list of e.g. [c1 c2] connected clauses
          adjacency-tuples (for [[from-node destinations] adjacency-list
                                 [to-node _] destinations];
                             [from-node to-node])
          ;; Just pick any clause to start with
          [first-clause & remaining-clauses] (keys named-clauses)]
      (loop [preds []
             xf-steps []
             covered-nodes #{first-clause}
             remaining-nodes (set remaining-clauses)
             ;; Locators tell me for a given clause where to find it in the joined tuple
             ;; So if the current join looks like [[[c1 c2] c3] c4]
             ;; The locator is~ {c1 (-> % first first first)
             ;;                  c2 (-> % first first second)
             ;;                  c3 (-> % first second)
             ;;                  c4 (-> % second)}
             locators {first-clause identity}
             n 1]

        (cond (empty? remaining-nodes)
              ;; We won! Prepend and append the joins with the boilerplate.
              (apply comp (concat
                           [(xf/mapcat-zset-transaction-xf)
                            (map (fn [zset]
                                   (apply xf/disj-irrelevant-items (cons zset preds))))]
                           xf-steps
                           [(xforms/reduce zs/zset+)]))

              ;; Stack overflow
              (> n 10)
              n

              :else
              (let [; Find me the first edge from any clause already included
                                        ; to any clause that hasn't been included yet.
                    [from to :as edge] (medley/find-first
                                        (fn [[from to]]
                                          (and (covered-nodes from)
                                               (remaining-nodes to)))
                                        adjacency-tuples)
                    ;; Look up what the variable is that links these clauses.
                    common-var (get-in adjacency-list [from to])
                    ;; Dereference the clause names to the clauses themselves.
                    [c1 c2] (map named-clauses edge)
                    ;; Generate predicates for checking join conditions.
                    [p1 p2] [(clause-pred (from locators) c1) (clause-pred identity c2)]
                    new-join (xf/join-xf p1
                                         ;; Index keys are just the clause locator composed with the getter from the datom.
                                         (comp (get-in variable-index [common-var from]) (from locators))
                                         p2
                                         ;; Look up the getter out of the datom.
                                         (get-in variable-index [common-var to])
                                         state
                                         ;; It's the last join when this is the last clause.
                                         :last? (= #{to} remaining-nodes))]
                (recur (conj preds p1 p2)
                       (conj xf-steps new-join)
                       (conj covered-nodes to)
                       (disj remaining-nodes to)
                       ;; As we lift this join to the left hand of the next step,
                       ;; all of the locators need to be composed with `safe-first`.
                       (-> (medley/map-vals #(comp % safe-first) locators)
                           (assoc to safe-second)
                           )
                       (inc n))))))))

(defmacro where-xf-macro [datalog-query]
  (let [where-clauses# (query->where-clauses datalog-query)
        named-clauses# (name-clauses where-clauses#)
        variable-index# (index-variables named-clauses#)
        adjacency-list# (build-adjacency-list named-clauses#)
        adjacency-tuples# (for [[from-node destinations] adjacency-list#
                                [to-node _] destinations];
                            [from-node to-node])
        [first-clause# & remaining-clauses#] (keys named-clauses#)
        state (gensym 'state) ]
    (loop [preds# []
           xf-steps# []
           covered-nodes# #{first-clause#}
           remaining-nodes# (set remaining-clauses#)
           locators# {first-clause# [identity]}
           n# 1]

      (cond (empty? remaining-nodes#)
            `(fn [~state]
               (comp
                (xf/mapcat-zset-transaction-xf)
                (map (fn [zset#]
                       (xf/disj-irrelevant-items zset# ~@preds#)))
                ~@xf-steps#
                (xforms/reduce zs/zset+)))

            ;; Stack overflow
            (> n# 10)
            n#

            :else
            (let [[from# to# :as edge#] (medley/find-first
                                         (fn [[from to]]
                                           (and (covered-nodes# from)
                                                (remaining-nodes# to)))
                                         adjacency-tuples#)

                  common-var# (get-in adjacency-list# [from# to#])
                  [ [e1# a1# v1# :as c1#] [e2# a2# v2# :as c2#]] (map named-clauses# edge#)
                  p1 (if (parser/variable? v1#)
                       `#(ds/datom-attr=  ((comp  ~@(from# locators#)) %) ~a1#)
                       `#(ds/datom-attr-val= ((comp ~@(from# locators#)) %) ~a1# ~v1#))
                  p2 (if (parser/variable? v2#)
                       `#(ds/datom-attr= % ~a2#)
                       `#(ds/datom-attr-val= % ~a2# ~v2#))
                  new-join `(xf/join-xf ~p1
                                        (comp ~(get-in variable-index# [common-var# from#]) ~@(from# locators#))
                                        ~p2
                                        ~(get-in variable-index# [common-var# to#])
                                        ~state
                                        :last? ~(= #{to#} remaining-nodes#))]
              (recur (conj preds# p1 p2)
                     (conj xf-steps# new-join)
                     (conj covered-nodes# to#)
                     (disj remaining-nodes# to#)
                     (-> (medley/map-vals #(conj % safe-first) locators#)
                         (assoc to# [safe-second]))
                     (inc n#)))))))

(deftest test-robocop-with-query-api "basic datalog query, with internal query api"
  (let [txn-atom (atom [])
        _conn           (load-learn-db txn-atom)
        query           (q/create-query
                         (where-xf-macro [:find ?name
                                          :where
                                          [?p :person/name ?name]
                                          [?m :movie/title "RoboCop"]
                                          [?m :movie/director ?p]]))]
    (is
     (=
      (q/input query @txn-atom)
      (q/get-result query)
      #{^#:zset{:w 1}
        [[[16 :person/name "Paul Verhoeven"]
          [59 :movie/director 16]]
         [59 :movie/title "RoboCop"]]}))))

(deftest test-ahhnold "Another basic query"
  (let [txn-atom (atom [])
        _conn (load-learn-db txn-atom)
        query (q/create-query
               (where-xf-macro
                [:find ?name
                 :where
                 [?m :movie/cast ?p]
                 [?p :person/name "Arnold Schwarzenegger"]
                 [?m :movie/director ?d]
                 [?d :person/name ?name]]))]

    (is (= #{"Jonathan Mostow" "James Cameron" "John McTiernan" "Mark L. Lester"}
           (transduce
            (map (comp ds/datom->val second))
            (completing #(conj %1 %2))
            #{}
            (q/input query @txn-atom))))))

(deftest test-function-vs-macro "where-xf versions"
  (let [txn-atom (atom [])
        _conn (load-learn-db txn-atom)
        query1 (q/create-query
               (where-xf-macro
                [:find ?name
                 :where
                 [?m :movie/cast ?p]
                 [?p :person/name "Arnold Schwarzenegger"]
                 [?m :movie/director ?d]
                 [?d :person/name ?name]]))
        query2 (q/create-query
                (where-xf
                 '[:find ?name
                   :where
                   [?m :movie/cast ?p]
                   [?p :person/name "Arnold Schwarzenegger"]
                   [?m :movie/director ?d]
                   [?d :person/name ?name]]))
        get-result-set (fn [query ipt]
                         (transduce
                          (map (comp ds/datom->val second))
                          (completing #(conj %1 %2))
                          #{}
                          (q/input query ipt)))]

    (is (= #{"Jonathan Mostow" "James Cameron" "John McTiernan" "Mark L. Lester"}
           (get-result-set query1 @txn-atom)
           (get-result-set query2 @txn-atom)))))

(comment
  (set! *print-meta* false)
  )
