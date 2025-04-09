(ns org.zsxf.datom2
  (:require [org.zsxf.util :as util]
            #?(:clj [org.zsxf.type :as t]))
  (:import (org.zsxf.type Datom2)))


(defn datom2
  ([datascript-datom]
   (datom2 datascript-datom nil))
  ([datascript-datom metadata]
   #?(:clj
      (t/->Datom2 datascript-datom metadata)
      :cljs
      ;TODO implement Datom2 for CLJS
      (let [[e a v _t add-or-retract :as datom] datascript-datom]
        [e a v]))))

(defn datom-like? [x]
  (boolean
    (and (util/nth2 x 0) (util/nth2 x 1) (util/nth2 x 2))))

(defn datom? [x]
  #?(:clj
     (instance? Datom2 x)
     :cljs
     ;TODO implement Datom2 for CLJS
     (datom-like? x)))
