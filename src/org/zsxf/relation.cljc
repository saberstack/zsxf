(ns org.zsxf.relation
  (:require [org.zsxf.util :as util]
            [org.zsxf.xf :as-alias xf]
            [taoensso.timbre :as timbre]))


(defn relation? [x]
  (true? (::xf/relation (meta x))))

(defn mark-as-rel
  "Mark x (typically a vector) as a relation via metadata"
  [x]
  (vary-meta x (fn [m] (assoc m ::xf/relation true))))

(defn type-not-found? [x]
  (and
    (relation? x)
    (= :not-found (peek x))))
