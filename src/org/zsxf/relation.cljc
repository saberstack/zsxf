(ns org.zsxf.relation
  (:require [org.zsxf.xf :as-alias xf]))

;TODO explore "associative" relation idea

(defn relation? [x]
  (boolean
    (and
      (indexed? x)
      (= (count x) 2)
      (:xf.clause (meta (first x)))
      (:xf.clause (meta (second x))))))
