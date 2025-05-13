(ns org.zsxf.datalog.compiler
  (:require [medley.core :as medley]
            [org.zsxf.datalog.parser :as parser]
            [org.zsxf.datalog.fn :as dfn]
            [org.zsxf.datalog.parser.spec :as parser-spec]
            [org.zsxf.datom :as d2]
            [org.zsxf.datalog.macro-util :as mu]
            [org.zsxf.zset :as zs]
            [org.zsxf.xf :as xf]
            [clojure.spec.alpha :as s]
            [net.cgrand.xforms :as xforms]
            [taoensso.timbre :as timbre]))

(def pos->getter
  {:entity `d2/datom->eid
   :value  `d2/datom->val})

(defn clause-pred [_ a v]
  (if (parser/variable? v)
    `#(d2/datom-attr= % ~a)
    `#(d2/datom-attr-val= % ~a ~v)))

(defn path-f [[f & _ :as locator-vec]]
  (condp = (count locator-vec)
    0
    `identity

    1
    `~f

    `(comp ~@locator-vec)))

(defn join-isolated-components [named-clauses locators state connected-components ]
  (loop
      [[component-1 component-2 & _] connected-components
       new-joins []
       locators locators]
    (if (nil? component-2)
      {:cartesian-joins new-joins
       :locators locators}

      (let [[c1-name#  c2-name# :as clause-names] (map first connected-components)
            [c1# c2#] (map named-clauses clause-names)
            cartesian-join `(xf/cartesian-xf
                             {:clause (quote ~c1#)
                              :pred ~(apply clause-pred c1#)
                              :path ~(path-f (c1-name# locators))}
                             {:clause (quote ~c2#)
                              :pred ~(apply clause-pred c2#)
                              :path ~(path-f (c2-name# locators))}
                             ~state)]
        (recur (next connected-components)
               (conj new-joins cartesian-join)
               (merge locators
                      (update-vals (select-keys locators component-1) #(conj % `mu/safe-first))
                      (update-vals (select-keys locators component-2) #(conj % `mu/safe-second))))))))


(defn* handle-single-clause
  "Handle special/trivial case of a single-clause query, by joining the clause to itself."
  [xf-steps predicates locators state variable-index named-clauses]
  (if (not= xf-steps [[]])
    [xf-steps predicates locators]
    (let [clause-name (first (keys named-clauses))
          [e1 a1 v1 :as c1] (get named-clauses clause-name)
          _ (do
              (assert (= 1 (count (keys named-clauses))) "This query has only one where clause.")
              (assert (or (parser/variable? e1)
                          (parser/variable? v1)) "There is a variable in the where body."))
          pred (clause-pred e1 a1 v1)
          var-position (-> variable-index vals first (get clause-name))]
      [[[`(xf/join-xf
           {:clause (quote ~c1)
            :pred ~pred
            :path identity
            :index-kfn ~(var-position pos->getter)}
           {:clause (quote ~c1)
            :pred ~pred
            :path identity
            :index-kfn ~(var-position pos->getter)}
           ~state)]]
       [pred]
       (update locators clause-name conj mu/safe-first)])))

(defn mark-last [xf-steps-flat]
  (let [last-index (dec (count xf-steps-flat))]
    (-> xf-steps-flat
        vec
        (update last-index concat [:last? true]))))

(defn var-to-getter [variable-index locators zset-item form]
  (if (parser/variable? form)
    (let [[[clause-to-select position] & _] (form variable-index)]
      `((comp ~(position pos->getter) ~@(clause-to-select locators)) ~zset-item))
    form))

(defn substitute-operator [[op & tail]]
  {:pre [(contains? dfn/query-fns op)]}
  (let [[_ fq-symbol] (find dfn/query-fns op)]
    (cons fq-symbol tail)))

(defn add-predicates [xf-steps-flat variable-index locators predicate-clauses]
  (if (empty? predicate-clauses)
    xf-steps-flat
    (let [pred-fns
          (map (fn [[predicate-clause]]
                 (let [zset-item (gensym 'zset-item)]
                   `(filter (fn [~zset-item]
                              (~@(->> predicate-clause
                                      (substitute-operator)
                                      (map (partial var-to-getter variable-index locators zset-item))))))))
               predicate-clauses)
          xf `(comp ~@pred-fns)
          last-index (dec (count xf-steps-flat))]
      (update xf-steps-flat last-index
              concat [:return-zset-item-xf xf]))))

(defn pre-parse-query
  "Accept quoted and non-quoted queries, i.e. '[:find ...] and [:find ...]"
  [query]
  (if (= 'quote (first query)) (first (rest query)) query))

(defmacro static-compile [query]
 (let [query (pre-parse-query query)]
  (condp = (s/conform ::parser-spec/query query)
    ::s/invalid
    `(ex-info "Invalid or unsupported query"
              {:explain-data ~(s/explain-data ::parser-spec/query query)})
    (let [{where-clauses# :where find-vars# :find} (parser/query->map query)
          {predicate-clauses# true pattern-clauses# false} (group-by (partial s/valid? ::parser-spec/predicate) where-clauses#)
          named-clauses# (parser/name-clauses pattern-clauses#)
          variable-index# (parser/index-variables named-clauses#)
          adjacency-list# (parser/build-adjacency-list named-clauses#)
          adjacency-tuples# (for [[from-node destinations] adjacency-list#
                                  [to-node _] destinations];
                              [from-node to-node])
          [first-component# & remaining-components# :as connected-components#]
          (parser/find-connected-components adjacency-list#)

          [first-clause# & remaining-clauses# ] first-component#
          state (gensym 'state)]
      (loop [preds# #{}
             xf-steps# [[]]
             covered-nodes# #{first-clause#}
             remaining-nodes# (set remaining-clauses#)
             locators# {first-clause# []}
             remaining-components# remaining-components#
             n# 1]

        (cond (and (empty? remaining-nodes#) (empty? remaining-components#))
              (let [[xf-steps# preds# locators#] (handle-single-clause xf-steps# preds# locators# state variable-index# named-clauses#)
                    {:keys [locators cartesian-joins]} (join-isolated-components named-clauses# locators# state connected-components#)
                    xf-steps-flat# (-> (if (empty? cartesian-joins)
                                          (first xf-steps#)
                                          (->> xf-steps# (cons cartesian-joins) reverse vec (apply concat)))
                                        (mark-last)
                                        (add-predicates variable-index# locators predicate-clauses#))]
                `(fn [~state]
                  (comp
                   (xf/mapcat-zset-transaction-xf)
                   (map (fn [zset#]
                          (xf/disj-irrelevant-items zset# ~@preds#)))
                   ~@xf-steps-flat#
                   (xforms/reduce (zs/zset-xf+ (map
                                                (xf/same-meta-f
                                                  (juxt ~@(map (fn [find-var#]
                                                                 (let [[[clause-to-select# position#] & _] (find-var# variable-index#)]
                                                                   `(comp ~(position# pos->getter) ~@(clause-to-select# locators))))
                                                               find-vars#)))))))))

              ;; Stack overflow
              (> n# 100)
              {:remaining-nodes remaining-nodes#
               :remaining-components remaining-components#
               :xf-steps xf-steps#
               :n n#}

              (and (empty? remaining-nodes#) (seq remaining-components#))
              (let [next-component# (first remaining-components#)
                    next-node# (first next-component#)]
                (recur preds#
                       (vec (cons [] xf-steps#))
                       #{next-node#}
                       (disj next-component# next-node#)
                       locators#
                       (next remaining-components#)
                       n#))

              :else
              (let [[from# to# :as edge#] (medley/find-first
                                           (fn [[from to]]
                                             (and (covered-nodes# from)
                                                  (remaining-nodes# to)))
                                           adjacency-tuples#)

                    common-var# (get-in adjacency-list# [from# to#])
                    [[e1# a1# v1# :as c1#] [e2# a2# v2# :as c2#]] (map named-clauses# edge#)
                    pred1# (clause-pred e1# a1# v1#)
                    pred2# (clause-pred e2# a2# v2#)
                    locator-vec# (from# locators#)
                    new-join# `(xf/join-xf
                               {:clause (quote ~c1#)
                                :pred ~pred1#
                                :path ~(path-f locator-vec#)
                                :index-kfn ~((get-in variable-index# [common-var# from#]) pos->getter)}
                               {:clause (quote ~c2#)
                                :pred ~pred2#
                                :path identity
                                :index-kfn ~((get-in variable-index# [common-var# to#]) pos->getter) }
                               ~state)]
                (recur (conj preds# pred1# pred2#)
                       (update xf-steps# 0 conj new-join#)
                       (conj covered-nodes# to#)
                       (disj remaining-nodes# to#)
                       (merge {from# [`mu/safe-first]}
                              locators#
                              (update-vals (select-keys locators# covered-nodes#) #(conj % `mu/safe-first))
                              {to# [`mu/safe-second]})
                       remaining-components#
                       (inc n#)))))))))

(defn runtime-compile
  "Runtime compile a datalog query.
  Returns a ZSXF-compatible transducer.

  IMPORTANT: Use only with trusted sources.
  runtime-compile can execute arbitrary code."
  [query]
  (eval `(static-compile ~query)))

(comment
  ;Runtime compilation of a query
  ; Use only with trusted sources!
  ; Example of a random code execution:
  (runtime-compile
    (do
      (println "executing!")
      '[:find ?a ?c
        :where
        [?a :b ?c]])))
