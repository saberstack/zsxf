(ns org.zsxf.experimental.spec
  (:require [org.zsxf.xf :as-alias xf]
            [clojure.spec.alpha :as s]))

(s/def ::xf/path fn?)
(s/def ::xf/clause vector?)
(s/def ::xf/index-kfn fn?)
(s/def ::xf/pred fn?)
(s/def ::xf/join-xf-rel-arg-map
  (s/keys :req [::xf/clause ::xf/pred ::xf/index-kfn]))

(s/def :datom/e pos-int?)
(s/def :datom/a keyword?)
(s/def :datom/v (s/or :string string? :integer pos-int?))
(s/def :datom/datom (s/tuple :datom/e :datom/a :datom/v))

(comment
  ;generating datoms poc
  (s/exercise :datom/datom 30))
