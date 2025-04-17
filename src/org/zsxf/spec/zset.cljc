(ns org.zsxf.spec.zset
  (:require [clojure.spec.alpha :as s]
            [clojure.test.check.generators :as gen]
            [org.zsxf.zset :as-alias zs]))

;different from others so it prints shorter by default in metadata
(s/def :zset/w (s/or
                 :pos-int pos-int?
                 :neg-int neg-int?))

(s/def ::zs/has-metadata?
  (s/with-gen
    (fn [x] (and
              (some? (meta x))
              (s/valid? :zset/w (:zset/w (meta x)))))
    (fn []
      (gen/let [base     (gen/one-of [(gen/vector gen/small-integer)
                                      (gen/map gen/keyword gen/small-integer)
                                      (gen/set gen/small-integer)
                                      gen/symbol])
                meta-map (gen/hash-map :zset/w (s/gen :zset/w))]
        (with-meta base meta-map)))))

(s/def ::zs/zset-item
  (s/and
    ::zs/has-metadata?
    (s/or :coll coll? :sym symbol?)))

(s/def ::zs/zset
  (s/coll-of ::zs/zset-item :kind set?))
