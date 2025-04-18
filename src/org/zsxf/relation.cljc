(ns org.zsxf.relation
  (:require [org.zsxf.util :as util]
            [org.zsxf.xf :as-alias xf]
            [taoensso.timbre :as timbre]))


(defn relation? [x]
  (true? (::xf/relation (meta x))))

(defn optional? [x]
  (and (relation? x)
    (true? (::xf/relation.opt (meta x)))))

(defn mark-as-rel
  "Mark x (typically a vector) as a relation via metadata"
  [x]
  (vary-meta x (fn [m] (assoc m ::xf/relation true))))

;TODO remove?
(defn mark-as-opt-rel
  "Mark x (typically a vector) as a relation via metadata"
  [x]
  (vary-meta x (fn [m] (assoc m
                         ::xf/relation true
                         ::xf/relation.opt true))))

(defonce not-found [:not-found])

(defn type-not-found? [x]
  (and
    (relation? x)
    (= not-found (peek x))))

;TODO remove?
(defn rel->not-found [rel]
  (if (= not-found (-> (util/nth2 rel 0) (util/nth2 1)))
    (recur (util/nth2 rel 0))
    (update rel 1 (fn [_] not-found))))

(comment
  (rel->not-found [[:A :B] [:not-found]])

  (rel->not-found [[[1 :movie/title "The Terminator"] [:not-found]] [:not-found]])
  (rel->not-found [[[:A :B] [:not-found]] [:not-found]])
  (rel->not-found [[[:A :B] :C] [:not-found]]))

(defn maybe-zsi->not-found [zsi]
  (when (optional? zsi) (rel->not-found zsi)))
