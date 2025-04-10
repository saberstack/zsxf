(ns org.zsxf.datalog.compiler
  (:require [medley.core :as medley]
            [org.zsxf.datalog.parser :as parser]
            [org.zsxf.datom2 :as d2]
            [org.zsxf.zset :as zs]
            [org.zsxf.xf :as xf]
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

(defmacro sprinkle-dbsp-on [datalog-query]
  (let [{where-clauses# :where find-vars# :find} (parser/query->map datalog-query)
        named-clauses# (parser/name-clauses where-clauses#)
        variable-index# (parser/index-variables named-clauses#)
        adjacency-list# (parser/build-adjacency-list named-clauses#)
        adjacency-tuples# (for [[from-node destinations] adjacency-list#
                                [to-node _] destinations];
                            [from-node to-node])
        [first-clause# & remaining-clauses#] (keys named-clauses#)
        state (gensym 'state) ]
    (loop [preds# #{}
           xf-steps# []
           covered-nodes# #{first-clause#}
           remaining-nodes# (set remaining-clauses#)
           locators# {first-clause# []}
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
                                                              (let [[[clause-to-select# position#] & _] (find-var# variable-index#)]
                                                                `(comp ~(position# pos->getter) ~@(clause-to-select# locators#))))
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
                  [[e1# a1# v1# :as c1#] [e2# a2# v2# :as c2#]] (map named-clauses# edge#)
                  pred1# (clause-pred e1# a1# v1#)
                  pred2# (clause-pred e2# a2# v2#)
                  locator-vec# (from# locators#)
                  new-join `(xf/join-xf
                             {:clause (quote ~c1#)
                              :pred ~pred1#
                              :path ~(path-f locator-vec#)
                              :index-kfn ~((get-in variable-index# [common-var# from#]) pos->getter)}
                             {:clause (quote ~c2#)
                              :pred ~pred2#
                              :path identity
                              :index-kfn ~((get-in variable-index# [common-var# to#]) pos->getter) }
                             ~state
                             :last? ~(= #{to#} remaining-nodes#))]
              (recur (conj preds# pred1# pred2#)
                     (conj xf-steps# new-join)
                     (conj covered-nodes# to#)
                     (disj remaining-nodes# to#)
                     (-> (medley/map-vals #(conj % `safe-first) locators#)
                         (assoc to# [`safe-second]))
                     (inc n#)))))))

(defn runtime-compile
  "Runtime compile a datalog query.
  Returns a ZSXf-compatible transducer.

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
