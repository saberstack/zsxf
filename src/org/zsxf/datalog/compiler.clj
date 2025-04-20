(ns org.zsxf.datalog.compiler
  (:require [medley.core :as medley]
            [org.zsxf.datalog.parser :as parser]
            [org.zsxf.datalog.parser.spec :as parser-spec]
            [org.zsxf.datom2 :as d2]
            [org.zsxf.zset :as zs]
            [org.zsxf.xf :as xf]
            [clojure.spec.alpha :as s]
            [datascript.built-ins :as built-ins]
            [net.cgrand.xforms :as xforms]
            [taoensso.timbre :as timbre]))

(def pos->getter
  {:entity `d2/datom->eid
   :value  `d2/datom->val})

(defn safe-first [thing]
  (when (vector? thing)
    (first thing)))

(defn safe-second [thing]
  (when (vector? thing)
    (second thing)))

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
                      (update-vals (select-keys locators component-1) #(conj % `safe-first))
                      (update-vals (select-keys locators component-2) #(conj % `safe-second))))))))

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
  {:pre [(contains? built-ins/query-fns op)]}
  (cons (built-ins/query-fns op) tail))

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

(defmacro sprinkle-dbsp-on [query]
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
              (let [{:keys [locators cartesian-joins]} (join-isolated-components named-clauses# locators# state connected-components#)
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
              (> n# 15)
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
                       (merge {from# [`safe-first]}
                              locators#
                              (update-vals (select-keys locators# covered-nodes#) #(conj % `safe-first))
                              {to# [`safe-second]})
                       remaining-components#
                       (inc n#))))))))

(defn runtime-compile
  "Runtime compile a datalog query.
  Returns a ZSXF-compatible transducer.

  IMPORTANT: Use only with trusted sources.
  runtime-compile can execute arbitrary code."
  [datalog-query]
  (eval `(sprinkle-dbsp-on ~datalog-query)))

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
