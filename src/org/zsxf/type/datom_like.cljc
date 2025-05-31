(ns org.zsxf.type.datom-like)

(defprotocol DatomLike
  "Protocol for types that can be used as datoms in ZSXF.
  This is a marker protocol, not intended to be implemented directly."
  (datom-like? [x]
    "Returns true if x is a valid datom-like object.")
  )
