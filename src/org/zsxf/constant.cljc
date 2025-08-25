(ns org.zsxf.constant)

;Optimization to save (a lot!) of memory.
;Reuse common zset weight maps
(defonce zset-count [:zset/count])

(defonce zset-sum [:zset/sum])

(defmacro zset-weight-of-1 []
  {:zset/w 1})
