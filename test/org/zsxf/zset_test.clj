(ns org.zsxf.zset-test
  (:require
   [org.zsxf.zset :as zs]
   [clojure.test :refer :all]))

(deftest test-1-indexed-zset-pos+
  (let [indexed-zset
        (zs/indexed-zset-pos+
          {1 (zs/zset [[1]])}
          {1 (zs/zset [[1]])})]
    (is (= indexed-zset {1 #{^#:zset{:w 2} [1]}}))))

(deftest test-2-indexed-zset-pos+
  (let [indexed-zset
        (zs/indexed-zset-pos+
          {1 (zs/zset [[1]])}
          {1 (zs/zset-negative [[1] [2]])})]
    (is (= indexed-zset {}))))

(deftest test-3-indexed-zset-pos+
  (let [indexed-zset
        (zs/indexed-zset-pos+
          {1 (zs/zset [[1]])}
          {1 (zs/zset-negative [[1] [2]])
           2 (zs/zset [[2]])})]
    (is (= indexed-zset {2 #{^#:zset{:w 1} [2]}}))))
