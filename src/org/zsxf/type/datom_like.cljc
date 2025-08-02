(ns org.zsxf.type.datom-like
  (:require [org.zsxf.util :as util]))

(defprotocol DatomLike
  "Protocol for types that can be used as datoms in ZSXF.
  This is a marker protocol, not intended to be implemented directly."
  (-datom-like? [x]
    "Returns true if x is a valid datom-like object.")
  )

(defn datom-like?
  "Checks if x is a valid datom-like object.
  This function is used to determine if an object can be treated as a datom in ZSXF.
  Use instance? for performance reasons."
  [x]
  #?(:clj (instance? org.zsxf.type.datom_like.DatomLike x)
     ;TODO implement for CLJS if needed
     :cljs (util/datom-like-structure? x)))
