(ns org.zsxf.xf
  "ZSXF-compatible transducers (they receive and return zsets)

    # Glossary

   `Clause`: defines a relation, or a part of a relation

   `Set of unified clauses`: a set of clauses that are unified via common variables
     Example:
      [?m :movie/direction ?p]
      [?p :person/name ?name]
     ... is unified by the common variable ?p

   `Joined pair`: a pair of relations, optionally nested
     [:R1 :R2] ;two relations (smallest possible joined pair)
     [[:R1 :R2] :R3] ;three relations (still a pair!)
     [[[:R1 :R2] :R3] :R4] ;four relations (still a pair!)
     ... etc."
  (:require [net.cgrand.xforms :as xforms]
            [org.zsxf.constant :as const]
            [org.zsxf.zset :as zs]
            [org.zsxf.xf :as-alias xf]
            [org.zsxf.relation :as rel]
            [taoensso.timbre :as timbre]
            #?(:clj [org.zsxf.type :as t])                  ;don't remove! type import fails
            [org.zsxf.util :as util])
  #?(:clj
     (:import (org.zsxf.type Datom2))))

(defn rf-branchable
  "Helper to adapt a reducing function to a branching transducer.

  Don't pass the completing of the rf through because completing multiple times
  is invalid and this transducer will do that after its child xforms have been
  completed."
  [rf]
  (fn
    ([result] result)
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn detect-join-type [zset-item path-f clause]
  (cond
    #?(:clj  (instance? Datom2 zset-item)
       :cljs (util/datom-like? zset-item)) :datom
    (rel/relation? zset-item) :relation
    :else
    (throw (ex-info "invalid input zset-item" {:zset-item           zset-item
                                               :path-f-of-zset-item (path-f zset-item)
                                               :clause              clause}))))

(defn can-join?
  [zset-item path-f clause]
  (let [jt             (detect-join-type zset-item path-f clause)
        item-can-join? (condp = jt
                         :datom true
                         :relation (= clause (:xf.clause (meta (path-f zset-item)))))]
    #_(when (false? item-can-join?)
        (timbre/info "cannot join !!!")
        (timbre/info zset-item)
        (timbre/info "path-f to item ::::" (path-f zset-item))
        (timbre/info "zset-item clause is" (:xf.clause (meta (path-f zset-item))))
        (timbre/info "zset-item meta ::::" (meta (path-f zset-item)))
        (timbre/info "looking for clause:" clause))
    item-can-join?))

(defn with-clause-f [clause]
  (fn [item]
    (vary-meta item (fn [m] (assoc m :xf.clause clause)))))

(defn- join-xf-impl
  [[index-state-1-prev index-state-2-prev [delta-1 delta-2 _zset]] [clause-1 clause-2]]
  (let [f1 (with-clause-f clause-1)
        f2 (with-clause-f clause-2)]
    (zs/indexed-zset+
      ;ΔA ⋈ B
      (zs/intersect-indexed* delta-1 index-state-2-prev f1 f2 rel/index-clauses)
      ;A ⋈ ΔB
      (zs/intersect-indexed* index-state-1-prev delta-2 f1 f2 rel/index-clauses)
      ;ΔA ⋈ ΔB
      (zs/intersect-indexed* delta-1 delta-2 f1 f2 rel/index-clauses))))

(defn join-xf
  "Receives:

    A single zset (at a time)

     Each zset is expanded (via mapcat) at the beginning of join-xf into
     zset-items and joined pairs from previous join-xfs outputs
     zset-items can be
     - `datoms`
     - `joined pairs`, i.e. [:R1 :R2], [[:R1 :R2] :R3] etc.

   Returns (via mapcat):

    Two zsets

    Each zset has one of:
    - new `joined pair`, i.e. [:R1 R2], or [[:R1 :R2] :R3], etc.
    - each input zset-item unchanged, unless ?last is true when an empty set is returned.
      The reason for passing through unchanged zset-items is to allow downstream transducers
      to (potentially) process them

    Options:

    - :return-zset-item-xf
      A transducer to transform each item (a datom or a joined pair)
      before final inclusion for downstream processing.
      Example:
      (map (fn [item] ...))
      (filter (fn [item] ...))"
  [{clause-1 :clause path-f-1 :path pred-1 :pred index-kfn-1 :index-kfn :or {path-f-1 identity}}
   {clause-2 :clause path-f-2 :path pred-2 :pred index-kfn-2 :index-kfn :or {path-f-2 identity}}
   query-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}}]
  (let [uuid-1 [clause-1 (random-uuid)]
        uuid-2 [clause-2 (random-uuid)]]
    (timbre/info uuid-1)
    (timbre/info uuid-2)
    (timbre/info [clause-1 clause-2])
    (comp
      ;receives a zset, unpacks zset into individual items
      (mapcat identity)
      ;receives a vector pair of zset-meta and zset-item (pair constructed in the previous step)
      (map (fn [zset-item]
             (let [delta-1 (if (and
                                 (can-join? zset-item path-f-1 clause-1)
                                 (pred-1 (path-f-1 zset-item)))
                             (zs/index #{zset-item} (comp index-kfn-1 path-f-1))
                             {})
                   delta-2 (if (and
                                 (can-join? zset-item path-f-2 clause-2)
                                 (pred-2 (path-f-2 zset-item)))
                             (zs/index #{zset-item} (comp index-kfn-2 path-f-2))
                             {})
                   zset    (if last? #{} #{zset-item})]
               ;return
               [delta-1 delta-2 zset])))
      (cond-branch
        ;does this join-xf care about the current item?
        (fn [[delta-1 delta-2 _zset :as _delta-1+delta-2+zset]]
          ;if none of the predicates were true...
          (and (empty? delta-1) (empty? delta-2)))
        (map (fn [[_delta-1 _delta-2 zset]]
               ;return the zset
               zset))
        ;else, proceed to join
        any?
        (comp

          (map (fn [[delta-1 delta-2 zset]]
                 (let [index-state-1-prev (get @query-state uuid-1 {})
                       index-state-2-prev (get @query-state uuid-2 {})]
                   ;advance indices
                   (swap! query-state
                     (fn [state]
                       (-> state
                         (update uuid-1 (fn [index] (zs/indexed-zset-pos+ index delta-1)))
                         (update uuid-2 (fn [index] (zs/indexed-zset-pos+ index delta-2))))))
                   ;return
                   [index-state-1-prev index-state-2-prev [delta-1 delta-2 zset]])))

          (map (fn [[index-state-1-prev index-state-2-prev [delta-1 delta-2 zset]]]
                 ;return
                 (vector
                   ;add :where clauses as metadata to the joined relations (a zset)
                   (zs/indexed-zset->zset
                     (join-xf-impl [index-state-1-prev index-state-2-prev [delta-1 delta-2 zset]] [clause-1 clause-2])
                     ;transducer to transform zset items during conversion indexed-zset -> zset
                     (comp return-zset-item-xf))
                   ;original zset-item wrapped in a zset
                   zset)))

          (mapcat (fn [[join-xf-delta zset]]
                    ;pass along to next xf join-xf-delta and zset, one at a time via mapcat
                    [join-xf-delta zset])))))))

(defn outer-join-xf []
  ;TODO continue here
  ; use join-xf, difference-xf, and union-xf
  )


(defonce tmp-state (atom nil))

(defn can-join-union?
  "Unions use a simpler can-join?
  Can only join to exact clause matches for the time being"
  [zset-item path-f clause]
  (let [item-can-join? (= clause (:xf.clause (meta (path-f zset-item))))]
    item-can-join?))

(defn union-xf
  [{clause-1 :clause path-f-1 :path pred-1 :pred item-f-1 :zset-item-f :or {path-f-1 identity}}
   {clause-2 :clause path-f-2 :path pred-2 :pred item-f-2 :zset-item-f :or {path-f-2 identity}}
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}}]
  (timbre/info "union-xf setup...")
  (comp
    ;receives a zset, unpacks zset into individual items
    (mapcat identity)
    ;receives a vector pair of zset-meta and zset-item (pair constructed in the previous step)
    (map (fn [-zset-item]
           (timbre/spy -zset-item)
           (let [delta-1 (if (and (can-join-union? -zset-item path-f-1 clause-1) (pred-1 (path-f-1 -zset-item)))
                           (zs/zset #{(zs/new-zset-item (item-f-1 -zset-item) (zs/zset-weight -zset-item))})
                           #{})
                 delta-2 (if (and (can-join-union? -zset-item path-f-2 clause-2) (pred-2 (path-f-2 -zset-item)))
                           (zs/zset #{(zs/new-zset-item (item-f-2 -zset-item) (zs/zset-weight -zset-item))})
                           #{})
                 zset    (if last? #{} #{-zset-item})]
             ;return
             (timbre/spy [delta-1 delta-2 zset]))))
    (cond-branch
      ;does difference-xf care about the current item?
      (fn [[delta-1 delta-2 _zset :as _delta-1+delta-2+zset]]
        ;if none of the predicates were true...
        (and (empty? delta-1) (empty? delta-2)))
      (map (fn [[_delta-1 _delta-2 zset]]
             ;no – return the zset and stop processing
             zset))
      ;else, proceed to join
      any?
      (comp
        (map (fn [[delta-1 delta-2 zset]]
               ;return
               (vector
                 (zs/zset+
                   (map identity)
                   #{} delta-1 delta-2)
                 ;original zset-item wrapped in a zset
                 zset)))
        (mapcat (fn [[union-xf-delta zset]]
                  ;pass along to next xf join-xf-delta and zset, one at a time via mapcat
                  (timbre/spy [union-xf-delta zset])))))))


(defn difference-xf
  [{clause-1 :clause path-f-1 :path pred-1 :pred item-f-1 :zset-item-f :or {path-f-1 identity}}
   {clause-2 :clause path-f-2 :path pred-2 :pred item-f-2 :zset-item-f :or {path-f-2 identity}}
   & {:keys [last? return-zset-item-xf output-clause]
      :or   {last? false return-zset-item-xf (map identity)}}]
  (comp
    ;receives a zset, unpacks zset into individual items
    (mapcat identity)
    ;receives a vector pair of zset-meta and zset-item (pair constructed in the previous step)
    (map (fn [-zset-item]
           (timbre/spy -zset-item)
           (let [delta-1 (if (and (can-join? -zset-item path-f-1 clause-1) (pred-1 (path-f-1 -zset-item)))
                           (zs/zset #{(zs/new-zset-item (item-f-1 -zset-item) (zs/zset-weight -zset-item))})
                           #{})
                 delta-2 (if (and (can-join? -zset-item path-f-2 clause-2) (pred-2 (path-f-2 -zset-item)))
                           (zs/zset #{(zs/new-zset-item (item-f-2 -zset-item) (zs/zset-weight -zset-item))})
                           #{})
                 zset    (if last? #{} #{-zset-item})]
             ;return
             (timbre/spy [delta-1 delta-2 zset]))))
    (cond-branch
      ;does difference-xf care about the current item?
      (fn [[delta-1 delta-2 _zset :as _delta-1+delta-2+zset]]
        ;if none of the predicates were true...
        (and (empty? delta-1) (empty? delta-2)))
      (map (fn [[_delta-1 _delta-2 zset]]
             ;no – return the zset and stop processing
             zset))
      ;else, proceed to join
      any?
      (comp
        (map (fn [[delta-1 delta-2 zset]]
               (timbre/spy [delta-1 delta-2 zset])
               ;return
               (vector
                 (zs/zset+
                   (map (fn [item]
                          (timbre/spy output-clause)
                          (if output-clause
                            ((with-clause-f output-clause) item)
                            item)))
                   #{} delta-1 (zs/zset-negate delta-2))
                 ;original zset-item wrapped in a zset
                 zset)))
        (mapcat (fn [[difference-xf-delta zset]]
                  ;pass along to next xf join-xf-delta and zset, one at a time via mapcat
                  (timbre/spy [difference-xf-delta zset])))))))

(defn cartesian-xf
  "Cartesian product, aka cross join"
  [{clause-1 :clause path-f-1 :path pred-1 :pred :or {path-f-1 identity}}
   {clause-2 :clause path-f-2 :path pred-2 :pred :or {path-f-2 identity}}
   query-state
   & {:keys [last? return-zset-item-xf]
      :or   {last?               false
             return-zset-item-xf (map identity)}}]
  (let [uuid-1 (random-uuid)
        uuid-2 (random-uuid)]
    (comp
      ;receives a zset, unpacks zset into individual items
      (mapcat identity)
      ;receives a vector pair of zset-meta and zset-item (pair constructed in the previous step)
      (map (fn [zset-item]
             (let [delta-1 (if (and
                                 (can-join? zset-item path-f-1 clause-1)
                                 (pred-1 (path-f-1 zset-item)))
                             #{zset-item}
                             {})
                   delta-2 (if (and
                                 (can-join? zset-item path-f-2 clause-2)
                                 (pred-2 (path-f-2 zset-item)))
                             #{zset-item}
                             {})
                   zset    (if last? #{} #{zset-item})]
               ;return
               [delta-1 delta-2 zset])))
      (cond-branch
        ;does this join-xf care about the current item?
        (fn [[delta-1 delta-2 _zset :as _delta-1+delta-2+zset]]
          ;if none of the predicates were true...
          (and (empty? delta-1) (empty? delta-2)))
        (map (fn [[_delta-1 _delta-2 zset]]
               ;return the zset
               zset))
        ;else, proceed to join
        any?
        (comp
          (map (fn [[delta-1 delta-2 zset]]
                 (let [sub-state-1-prev (get @query-state uuid-1 {})
                       sub-state-2-prev (get @query-state uuid-2 {})]
                   ;advance indices
                   (swap! query-state
                     (fn [state]
                       (-> state
                         (update uuid-1 (fn [index] (zs/zset-pos+ (or index #{}) delta-1)))
                         (update uuid-2 (fn [index] (zs/zset-pos+ (or index #{}) delta-2))))))
                   ;return
                   [sub-state-1-prev sub-state-2-prev [delta-1 delta-2 zset]])))
          (map (fn [[sub-state-1-prev sub-state-2-prev [delta-1 delta-2 zset]]]
                 (let [f1 (with-clause-f clause-1)
                       f2 (with-clause-f clause-2)]
                   ;return
                   (vector
                     (zs/zset+
                       return-zset-item-xf
                       #{}
                       ;ΔA ⋈ B
                       (zs/zset* delta-1 sub-state-2-prev f1 f2 rel/index-clauses)
                       ;A ⋈ ΔB
                       (zs/zset* sub-state-1-prev delta-2 f1 f2 rel/index-clauses)
                       ;ΔA ⋈ ΔB
                       (zs/zset* delta-1 delta-2 f1 f2 rel/index-clauses))
                     ;original zset-item wrapped in a zset
                     zset))))
          (mapcat (fn [[cartesian-xf-delta zset]]
                    ;pass along to next xf join-xf-delta and zset, one at a time via mapcat
                    [cartesian-xf-delta zset])))))))

(defn group-by-xf
  ;wip
  ([f xform]
   (group-by-xf f xform identity))
  ([f xform set-f]
   (comp
     (map (fn [zset] (zs/index zset f)))
     (map (fn [indexed-zset]
            (update-vals indexed-zset
              (fn [indexed-zset-item]
                (set-f
                  (into #{} xform indexed-zset-item)))))))))


(def group-by-count-xform
  (comp
    (xforms/transjuxt {:cnt (xforms/reduce zs/zset-count+)})
    (mapcat (fn [{:keys [cnt]}] [(zs/zset-count-item cnt)]))))

(defn same-meta-f
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
  (mapcat (fn [tx-v] tx-v)))

(defn disj-irrelevant-items [zset & preds]
  (into
    #{}
    (filter (apply some-fn preds))
    zset))
