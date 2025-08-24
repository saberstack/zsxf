(ns org.saberstack.clojure.inline)

(defmacro meta-vary-meta
  [meta-map obj f x y]
  `(with-meta ~obj (~f ~meta-map ~x ~y)))

(defmacro vary-meta-x
  [obj f x]
  `(with-meta ~obj (~f (meta ~obj) ~x)))

(defmacro vary-meta-xy
  [obj f x y]
  `(with-meta ~obj (~f (meta ~obj) ~x ~y)))
