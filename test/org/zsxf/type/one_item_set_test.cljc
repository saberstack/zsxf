(ns org.zsxf.type.one-item-set-test
  (:require
   #?(:clj  [clojure.test :refer [deftest is]]
      :cljs [cljs.test :refer-macros [deftest is]])
   [org.zsxf.type.one-item-set :as ois]
   [taoensso.timbre :as timbre]
   #?(:clj [clj-memory-meter.core :as mm])))


#?(:clj
   (deftest one-item-set-test
     (is (= (ois/set-of-1 1) #{1}))
     (is (= (ois/set-of-1 1) (ois/set-of-1 1)))
     (is (= (ois/set-of-1 1) (ois/optimize-set #{1})))
     (is (= (ois/set-of-1 1) (ois/optimize-set #{1})))
     (is (= #{} (disj (ois/set-of-1 1) 1)))
     (is (nil? ((ois/set-of-1 1) 2)))
     (is (= 2 ((ois/set-of-1 2) 2)))))

#?(:clj
   (deftest one-item-set-equivalence-test
     ;equals with clojure set
     (is (= #{1} (ois/set-of-1 1)))
     (is (apply = [(ois/set-of-1 :a) #{:a} #{:a} (ois/set-of-1 :a)]))
     (is (false? (apply = [(ois/set-of-1 :a) #{:a} #{:b} (ois/set-of-1 :a)])))))
