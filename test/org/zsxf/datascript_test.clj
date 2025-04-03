(ns org.zsxf.datascript-test
  (:require
   [clj-memory-meter.core :as mm]
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
   [taoensso.timbre :as timbre]))

(defn load-learn-db
  ([]
   (load-learn-db nil)
   )
  ([listen-atom]
   (let [schema (util/read-edn-file "resources/learndatalogtoday/schema_datascript.edn")
         data (util/read-edn-file   "resources/learndatalogtoday/data_datascript.edn")
         conn (d/create-conn schema)]
     (when listen-atom
       (data-stream/listen-datom-stream conn listen-atom ds/tx-datoms->zset))
     (d/transact! conn data)
     conn)))

(defn query->map [q]
  (second (reduce
           (fn [[current-segment acc] el]
                (if (keyword? el)
                  [el acc]
                  [current-segment (update acc current-segment (fnil conj []) el)]))
           [nil {}]
           q)))

(defn name-clauses
  "Takes the where clauses of a datalog query and gives each a name, e.g. :c1.
  Returns a map from name to clause."
  [where-clauses]
  (into {}
        (map-indexed (fn [idx el]
                       [(keyword (format "c%s" (inc idx))) el]))
        where-clauses))

(defn index-variables
  "Builds a map from each variable appearing in the where clauses
  to the clauses they appear in, and in which position.
  Returns map from variable to map from clause name to positional getter function."
  [named-clauses]
  (reduce
   (fn [acc [clause-name [e _ v]]]
     (cond-> acc
       (parser/variable? e)
       (update e (fnil assoc {}) clause-name ds/datom->eid )
       (parser/variable? v)
       (update v (fnil assoc {}) clause-name ds/datom->val)))
   {}
   named-clauses))

(defn build-adjacency-list
  "Where clauses in a datalog query can be modeled as the nodes
  of a graph. The edges are common variables between clauses.
  Returns a map from source clause name to a map from destination
  clause name to the common variable (edge)."
  [named-clauses]
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

(defmacro clause-pred-macro [locator-vec [e a v :as clause]]
  (if (parser/variable? v)
    `#(ds/datom-attr=  ((comp ~@locator-vec) %) ~a)
    `#(ds/datom-attr-val= ((comp ~@locator-vec) %) ~a ~v)))

(defmacro sprinkle-dbsp-on [datalog-query]
  (let [{where-clauses# :where find-vars# :find} (query->map datalog-query)
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
                (xforms/reduce (zs/zset-xf+ (map
                                             (xf/with-meta-f
                                               (juxt ~@(map (fn [find-var#]
                                                              (let [[[clause-to-select# getter#] & _] (find-var# variable-index#)]
                                                                `(comp ~getter# ~@(clause-to-select# locators#))))
                                                            find-vars#))))))))

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
                  [[_ a1# v1# ] [_ a2# v2#]] (map named-clauses# edge#)
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
                         (sprinkle-dbsp-on [:find ?name
                                            :where
                                            [?p :person/name ?name]
                                            [?m :movie/title "RoboCop"]
                                            [?m :movie/director ?p]]))]
    (is (= (q/input query @txn-atom)
           (q/get-result query)
           #{["Paul Verhoeven"]}))))

(deftest test-ahhnold "Another basic query"
  (let [txn-atom (atom [])
        _conn (load-learn-db txn-atom)
        query (q/create-query
               (sprinkle-dbsp-on
                [:find ?name
                 :where
                 [?m :movie/cast ?p]
                 [?p :person/name "Arnold Schwarzenegger"]
                 [?m :movie/director ?d]
                 [?d :person/name ?name]]))]
    (is (= #{["Mark L. Lester"]
             ["Jonathan Mostow"]
             ["John McTiernan"]
             ["James Cameron"]}
           (q/input query @txn-atom)))))

(deftest test-b "another ad-hoc query"
  (let [conn   (load-learn-db)
        query  (q/create-query
                 (sprinkle-dbsp-on [:find ?title ?year
                                    :where
                                    [?m :movie/title ?title]
                                    [?m :movie/year ?year]]))]
    (ds/init-query-with-conn query conn)
    (is
        (= (q/get-result query)
           (d/q '[:find ?title ?year
                  :where
                  [?m :movie/title ?title]
                  [?m :movie/year ?year]]
                @conn)
                #{["Lethal Weapon" 1987] ["Aliens" 1986]
                  ["The Terminator" 1984] ["Rambo: First Blood Part II" 1985]
                  ["Mad Max Beyond Thunderdome" 1985] ["Mad Max" 1979]
                  ["First Blood" 1982] ["Predator" 1987]
                  ["Terminator 2: Judgment Day" 1991] ["Predator 2" 1990]
                  ["Mad Max 2" 1981] ["Lethal Weapon 2" 1989]
                  ["Braveheart" 1995] ["Terminator 3: Rise of the Machines" 2003]
                  ["Commando" 1985] ["Die Hard" 1988]
                  ["Alien" 1979] ["RoboCop" 1987]
                  ["Rambo III" 1988] ["Lethal Weapon 3" 1992]}))))

(comment
  (set! *print-meta* false)


  )
