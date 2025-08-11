(ns org.zsxf.type.one-item-set-test
  (:require
   #?(:clj  [clojure.test :refer [deftest is]]
      :cljs [cljs.test :refer-macros [deftest is]])
   [org.zsxf.type.one-item-set :as ois]
   [taoensso.timbre :as timbre]
   #?(:clj [clj-memory-meter.core :as mm])))


#?(:clj
   (deftest one-item-set-test
     (is (= (ois/one-set 1) #{1}))
     (is (= (ois/one-set 1) (ois/one-set 1)))
     (is (= (ois/one-set 1) (ois/set #{1})))
     (is (= (ois/one-set 1) (ois/set #{1})))
     (is (= #{} (disj (ois/one-set 1) 1)))
     (is (nil? ((ois/one-set 1) 2)))
     (is (= 2 ((ois/one-set 2) 2)))))

#?(:clj
   (deftest one-item-set-equivalence-test
     ;equals with clojure set
     (is (= #{1} (ois/one-set 1)))
     (is (apply = [(ois/one-set :a) #{:a} #{:a} (ois/one-set :a)]))
     (is (false? (apply = [(ois/one-set :a) #{:a} #{:b} (ois/one-set :a)])))))
