(ns org.zsxf.datalog.compiler
  (:require [medley.core :as medley]
            [org.zsxf.datalog.parser :as parser]
            [org.zsxf.zset :as zs]
            [org.zsxf.xf :as xf]
            [net.cgrand.xforms :as xforms]
            [taoensso.timbre :as timbre]))

(def pos->getter
  {:entity `ds/datom->eid
   :value `ds/datom->val})

(defn safe-first [thing]
  (when (vector? thing)
    (first thing)))

(defn safe-second [thing]
  (when (vector? thing)
    (second thing)))


(defn clause-pred [[f & _ :as locator-vec] a v]
  (condp = (count locator-vec)
    0
    (if (parser/variable? v)
      `#(ds/datom-attr= % ~a)
      `#(ds/datom-attr-val= % ~a ~v))

    1
    (if (parser/variable? v)
      `#(ds/datom-attr= (~f %) ~a)
      `#(ds/datom-attr-val= (~f %) ~a ~v))

    (if (parser/variable? v)
      `#(ds/datom-attr= ((comp ~@locator-vec) %)  ~a)
      `#(ds/datom-attr-val= ((comp ~@locator-vec) %)  ~a ~v))))

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
    (loop [preds# []
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
                  [[_ a1# v1# ] [_ a2# v2#]] (map named-clauses# edge#)
                  locator-vec# (from# locators#)
                  p1# (clause-pred locator-vec# a1# v1#)
                  p2# (clause-pred [] a2# v2#)
                  new-join `(xf/join-xf ~p1#
                                        (comp ~((get-in variable-index# [common-var# from#]) pos->getter) ~@(from# locators#))
                                        ~p2#
                                        ~((get-in variable-index# [common-var# to#]) pos->getter)
                                        ~state
                                        :last? ~(= #{to#} remaining-nodes#))]
              (recur (conj preds# p1# p2#)
                     (conj xf-steps# new-join)
                     (conj covered-nodes# to#)
                     (disj remaining-nodes# to#)
                     (-> (medley/map-vals #(conj % `safe-first) locators#)
                         (assoc to# [`safe-second]))
                     (inc n#)))))))
