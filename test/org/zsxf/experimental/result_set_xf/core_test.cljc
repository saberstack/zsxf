(ns org.zsxf.experimental.result-set-xf.core-test
  (:require
   #?(:clj  [clojure.test :refer [deftest is]]
      :cljs [cljs.test :refer-macros [deftest is]])
   [org.zsxf.experimental.result-set-xf.core :as rsxf]))


(deftest result-set-xf-basic-pull
  ;different set of columns across rows – only some artists have country
  (let [result-set #{{:track/name "Route 66" :artist/country "USA" :artist/name "Alice" :db/id 42}
                     {:track/name "Route 66" :artist/country "Monaco" :artist/name "Bob" :db/id 43}
                     {:track/name "101" :artist/name "Clark" :db/id 44}}
        pattern    [:track/name {:track/artists [:db/id :artist/name :artist/country]}]]
    (is (= (rsxf/pull result-set pattern)
          [#:track{:name    "Route 66",
                    :artists [{:db/id 42, :artist/name "Alice", :artist/country "USA"}
                              {:db/id 43, :artist/name "Bob", :artist/country "Monaco"}]}
            #:track{:name "101", :artists [{:db/id 44, :artist/name "Clark"}]}]))))
