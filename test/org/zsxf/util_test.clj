(ns org.zsxf.util-test
  (:require [clojure.test :refer :all]
            [org.zsxf.util :as util]))

(deftest keep-every-nth-test-1
  (is
    (=
      (into [] (util/keep-every-nth 10) (range 100))
      [0 10 20 30 40 50 60 70 80 90])))
