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

(defn mark-as-opt-rel
  "Mark x (typically a vector) as a relation via metadata"
  [x]
  (vary-meta x (fn [m] (assoc m
                         ::xf/relation true
                         ::xf/relation.opt true))))

(defonce not-found [:not-found])

(defn rel->not-found [rel]
  (update rel 1 (fn [_] not-found)))

(comment
  (rel->not-found [:rel :rel2]))

(defn type-not-found? [x]
  (and
    (relation? x)
    (= not-found (peek x))))
