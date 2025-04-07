(ns org.zsxf.xf
  (:require [org.zsxf.type :as t]                           ;don't remove! type import fails
            [org.zsxf.zset :as zs]
            [org.zsxf.xf :as-alias xf]
            [taoensso.timbre :as timbre])
  #?(:clj
     (:import (org.zsxf.type Datom2))))

(defn rf-branchable
  "Helper to adapt a reducing function to a branching transducer.

  Don't pass the completing of the rf through because completing multiple times
  is invalid and this transducer will do that after its child xforms have been
  completed."
  [rf]
  (fn ([result] result)
    ([result item] (rf result item))))

(defn cond-branch
  "Will route data down the first path whose predicate is truthy. The results are merged.

  Predicates are regular functions. They are called on each element that flows into this
  transducer until one of the predicates passes, causing the data to be routed down that
  predicate's xform."
  [& pred-xform-pairs]
  (let [pairs (partition-all 2 pred-xform-pairs)]
    (if (seq pairs)
      (fn [rf]
        (let [pairs (mapv (fn [[pred xform]]
                            (if xform
                              ;; if pred is not a fn, treat it as either always true or always false.
                              [(if (ifn? pred) pred (constantly (boolean pred)))
                               (xform (rf-branchable rf))]
                              ;; treat sole trailing xform as else:
                              [(constantly true) (pred (rf-branchable rf))]))
                      pairs)]

          (fn
            ([] (doseq [[_ xform] pairs] (xform)) (rf))
            ([result]
             (rf (reduce (fn [result [_ xform]] (xform result)) result pairs)))
            ([result input]
             (loop [[[pred xform] & pairs] pairs]
               (if pred
                 (if (pred input)
                   (xform result input)
                   (recur pairs))
                 result))))))
      (map identity))))

(defn join-xf
  "Joins two relations (represented by zsets)
  based on predicates pred-1 and pred-2 and index key functions index-k-1 and index-k-2.
  index-state is an atom containing a map of index UUIDs (one for each of the two relations) to indexed zsets."
  [pred-1 index-k-1 pred-2 index-k-2 index-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}}]
  (let [index-uuid-1 (random-uuid)
        index-uuid-2 (random-uuid)]
    (timbre/spy index-uuid-1)
    (timbre/spy index-uuid-2)
    (comp
      ;receives a zset
      (mapcat identity)
      ;receives a zset item (with zset weight)
      (map (fn [zset-item]
             (let [delta-1 (if (pred-1 zset-item) (zs/index #{zset-item} index-k-1) {})
                   delta-2 (if (pred-2 zset-item) (zs/index #{zset-item} index-k-2) {})
                   zset    (if last? #{} #{zset-item})]
               ;return
               [delta-1 delta-2 zset])))
      (cond-branch
        ;does this join-xf care about the current item?
        (fn [[delta-1 delta-2 _zset :as delta-1+delta-2+zset]]
          (timbre/spy delta-1+delta-2+zset)
          ;if none of the predicates were true...
          (and (empty? delta-1) (empty? delta-2)))
        (map (fn [[_delta-1 _delta-2 zset]]
               (timbre/spy [last? zset])
               zset))
        ;else, proceed to join
        any?
        (comp
          (map (fn [[delta-1 delta-2 zset]]
                 (let [index-state-1-prev (get @index-state index-uuid-1 {})
                       index-state-2-prev (get @index-state index-uuid-2 {})]
                   ;advance indices
                   (swap! index-state
                     (fn [state]
                       (-> state
                         (update index-uuid-1 (fn [index] (timbre/spy (zs/indexed-zset-pos+ index delta-1))))
                         (update index-uuid-2 (fn [index] (timbre/spy (zs/indexed-zset-pos+ index delta-2)))))))
                   ;return
                   [index-state-1-prev index-state-2-prev [delta-1 delta-2 zset]])))
          (map (fn [[index-state-1-prev index-state-2-prev [delta-1 delta-2 zset]]]
                 (timbre/spy zset)
                 (timbre/spy [delta-1 index-state-2-prev])
                 (timbre/spy [index-state-1-prev delta-2])
                 ;return
                 (vector
                   (zs/indexed-zset->zset
                     (zs/indexed-zset+
                       ;ΔTeam ⋈ Players
                       (timbre/spy (zs/join-indexed* delta-1 index-state-2-prev))
                       ;Teams ⋈ ΔPlayers
                       (timbre/spy (zs/join-indexed* index-state-1-prev delta-2))
                       ;ΔTeams ⋈ ΔPlayers
                       (timbre/spy (zs/join-indexed* delta-1 delta-2)))
                     ;transducer to transform zset items during conversion indexed-zset -> zset
                     return-zset-item-xf)
                   zset)))
          (mapcat (fn [[join-xf-delta zset]]
                    ;pass along to next xf join-xf-delta and zset, one at a time via mapcat
                    [(timbre/spy join-xf-delta) zset])))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn input-zset-item-type [zset-item]
  (cond
    (and
      (vector? zset-item)
      (= 2 (count zset-item))
      (some? (::xf/clauses (meta zset-item)))) :relation
    (and
      (instance? Datom2 zset-item)
      (nil? (::xf/clauses (meta zset-item)))) :datom
    :else (throw (ex-info "invalid input-zset" {:input-zset zset-item}))))

(defn zset-item-can-join?
  [zset-item clause]
  (let [zset-item-type      (input-zset-item-type zset-item)
        _                   (timbre/info "zset-item-type" zset-item-type)
        item-clauses (::xf/clauses (meta zset-item))
        zset-item-can-join? (condp = zset-item-type
                              :relation (contains? item-clauses clause)
                              :datom true)]
    (when (false? zset-item-can-join?)
      (timbre/info "cannot join :::" zset-item)
      (timbre/info "item tagged with" item-clauses)
      (timbre/info "looking for" clause))
    zset-item-can-join?))

(defn join-xf-2
  "WIP"
  [clause-1 pred-1 index-kfn-1 clause-2 pred-2 index-kfn-2 index-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}}]
  (let [index-uuid-1    (with-meta [(random-uuid)] {::xf/clause-1 clause-1})
        index-uuid-2    (with-meta [(random-uuid)] {::xf/clause-1 clause-2})
        join-xf-clauses #{clause-1 clause-2}]
    (timbre/spy index-uuid-1)
    (timbre/spy index-uuid-2)
    (timbre/spy join-xf-clauses)
    (comp
      ;receives a zset, unpacks zset into individual items
      (mapcat identity)
      ;receives a vector pair of zset-meta and zset-item (pair constructed in the previous step)
      (map (fn [zset-item]

             (let [delta-1 (if (and
                                 (zset-item-can-join? zset-item clause-1)
                                 (pred-1 zset-item))
                             (zs/index #{zset-item} index-kfn-1)
                             {})
                   delta-2 (if (and
                                 (zset-item-can-join? zset-item clause-2)
                                 (pred-2 zset-item))
                             (zs/index #{zset-item} index-kfn-2)
                             {})
                   zset    (if last? #{} #{zset-item})]
               ;return
               (timbre/spy
                 [delta-1 delta-2 zset]))))
      (cond-branch
        ;does this join-xf care about the current item?
        (fn [[delta-1 delta-2 _zset :as delta-1+delta-2+zset]]
          (timbre/spy delta-1+delta-2+zset)
          ;if none of the predicates were true...
          (and (empty? delta-1) (empty? delta-2)))
        (map (fn [[_delta-1 _delta-2 zset]]
               (timbre/spy [last? zset])
               ;return the zset
               zset))
        ;else, proceed to join
        any?
        (comp
          (map (fn [[delta-1 delta-2 zset]]
                 (let [index-state-1-prev (get @index-state index-uuid-1 {})
                       index-state-2-prev (get @index-state index-uuid-2 {})]
                   ;advance indices
                   (swap! index-state
                     (fn [state]
                       (-> state
                         (update index-uuid-1 (fn [index] (timbre/spy (zs/indexed-zset-pos+ index delta-1))))
                         (update index-uuid-2 (fn [index] (timbre/spy (zs/indexed-zset-pos+ index delta-2)))))))
                   ;return
                   [index-state-1-prev index-state-2-prev [delta-1 delta-2 zset]])))
          (map (fn [[index-state-1-prev index-state-2-prev [delta-1 delta-2 zset]]]
                 (timbre/spy zset)
                 (timbre/spy [delta-1 index-state-2-prev])
                 (timbre/spy [index-state-1-prev delta-2])
                 ;return
                 (vector
                   ;add :where clauses as metadata to the joined relations (a zset)
                   (zs/indexed-zset->zset
                     (zs/indexed-zset+
                       ;ΔTeam ⋈ Players
                       (timbre/spy (zs/join-indexed* delta-1 index-state-2-prev))
                       ;Teams ⋈ ΔPlayers
                       (timbre/spy (zs/join-indexed* index-state-1-prev delta-2))
                       ;ΔTeams ⋈ ΔPlayers
                       (timbre/spy (zs/join-indexed* delta-1 delta-2)))
                     ;transducer to transform zset items during conversion indexed-zset -> zset
                     (comp
                       return-zset-item-xf
                       (map (fn [zset-item]
                              (vary-meta
                                zset-item
                                (fn [m]
                                  (when m
                                    (timbre/info "existing meta" m))
                                  (assoc m ::xf/clauses join-xf-clauses)))))))
                   zset)))
          (mapcat (fn [[join-xf-delta zset]]
                    ;pass along to next xf join-xf-delta and zset, one at a time via mapcat
                    [(timbre/spy join-xf-delta) zset])))))))

(defn group-by-count-xf
  "Takes a group-by-style function f and returns a transducer which

  1. indexes the zset by f
  2. counts the number of items in each group
  3. returns an indexed zset (a map) where the keys are the result of f and the values are the counts

  Counts are represented as special zsets containing only one item with the count as the weight"
  [f]
  (comp
    (map (fn [zset] (zs/index zset f)))
    (map (fn [indexed-zset]
           (update-vals indexed-zset
             (fn [indexed-zset-item]
               #{(zs/zset-count-item
                   (transduce (map zs/zset-weight) + indexed-zset-item))}))))))

(defn group-by-xf
  ;wip
  [f xform]
  (comp
    (map (fn [zset] (zs/index zset f)))
    (map (fn [indexed-zset]
           (update-vals indexed-zset
             (fn [indexed-zset-item]
               (into #{} xform indexed-zset-item)))))))


(defn join-right-pred-1-xf
  "Joins already joined relations with a new relation.
  Modifies pred-1 and index-k-1 to point to the joined relations' second relation."
  [pred-1 index-k-1 pred-2 index-k-2 index-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}
      :as   params-map}]
  (join-xf (comp pred-1 second) (comp index-k-1 second) pred-2 index-k-2 index-state params-map))


(defn join-left-pred-1-xf
  "Joins already joined relations with a new relation.
  Modifies pred-1 and index-k-1 to point to the joined relations' first relation."
  [pred-1 index-k-1 pred-2 index-k-2 index-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}
      :as   params-map}]
  (join-xf (comp pred-1 first) (comp index-k-1 first) pred-2 index-k-2 index-state params-map))


(defn join-right-pred-2-xf
  "Joins already joined relations with a new relation.
  Modifies pred-2 and index-k-2 to point to the joined relations' second relation."
  [pred-1 index-k-1 pred-2 index-k-2 index-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}
      :as   params-map}]
  (join-xf pred-1 index-k-1 (comp pred-2 second) (comp index-k-2 second) index-state params-map))


(defn join-left-pred-2-xf
  "Joins already joined relations with a new relation.
  Modifies pred-2 and index-k-2 to point to the joined relations' first relation."
  [pred-1 index-k-1 pred-2 index-k-2 index-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}
      :as   params-map}]
  (join-xf pred-1 index-k-1 (comp pred-2 first) (comp index-k-2 first) index-state params-map))


(defn with-meta-f
  "Takes a function f and returns a function which takes data and returns (f data) with the same meta"
  [f]
  (fn [data]
    (with-meta
      (f data)
      (meta data))))


(defn mapcat-zset-transaction-xf
  "Receives a transaction represented by a vectors of zsets.
  Returns zsets one by one"
  []
  (mapcat (fn [tx-v] (timbre/spy tx-v))))

(defn init-result [result result-delta]
  (if (nil? result)
    ;init
    (cond
      (map? result-delta) [{} zs/indexed-zset+]             ;for aggregates, allow negative weights
      (set? result-delta) [#{} zs/zset-pos+]                ;regular joins, no negative weight
      :else (throw (ex-info "result-delta must be either map or set"
                     {:result-delta result})))
    ;else, existing result
    (cond
      (and (map? result) (map? result-delta)) [result zs/indexed-zset+] ;for aggregates, allow negative weights
      (and (set? result) (set? result-delta)) [result zs/zset-pos+] ;regular joins no negative weights
      :else (throw (ex-info "result and result-delta together must be either maps or sets"
                     {:result result :result-delta result})))))


(defn disj-irrelevant-items [zset & preds]
  (into
    #{}
    (filter (apply some-fn preds))
    zset))

(defn pull-join-xf []
  ; A pull pattern is similar to a join but differs in important ways.
  ; Given a query like:
  '[:find (pull ?p [:person/name
                    {:person/born [:country/name]}])
    :where
    [?p :person/name ?name]
    [?m :movie/title _]
    [?m :movie/director ?p]]
  ; ... we can see that the pull starts as typical at ?p (entity id)
  ; It _pulls_ the following:
  ;   :person/name  – which was already requested in the main :where join
  ;   :person/born  - new piece of data... which is actually a ref (a join!)
  ;      ... which points to ...
  ;   :country/name - new data also.
  ;
  ; (!) An important difference between a :where join and a (pull ...) "join":
  ;
  ; Even when a certain (pull ?p ...) "lacks" any (or all!) of the data requested by the (pull ...) pattern
  ; the number of returned items in the result does not change (the query can return a list like
  '([nil] [nil])                                            ; ... if nothing in the pattern is found
  ; or if found (more typically):
  '([#:person{:born #:country{:name "USA"} :name "Alice"}]
    [#:person{:born #:country{:name "Monaco"} :name "Bob"}])
  ;
  ; More important differences likely exist, this is WIP.
  ;
  )


;Glossary
; - Clause: defines a single relation
; - Joined relation: a pair of relations with arbitrary depth of joined relations nested inside
;    [:R1 :R2] ;two relations (smallest possible joined relation pair)
;    [[:R1 :R2] :R3] ;three relations
;    [[[:R1 :R2] :R3] :R4] ;four relations
;
;
;*Every* join-xf:
; - takes a zset, outputs one or more zsets
; - takes zset-items & joined relations (from previous join-xfs outputs)
;    (!) Note: zset-items can be datoms but critically can also
;       can be joined relations (a vector pair):
;       [:R1 :R2]
;       [[:R1 :R2] :R3]
;       [[[:R1 :R2] :R3] :R4]
;       Notice the top level vector count is always two (i.e. it's a pair)
;       with more pairs potentially nested at every level
; - outputs joined relations based on predicates and index kfns
; - outputs zset-items, unchanged (until :last?)
