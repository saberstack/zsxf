(ns org.zsxf.util-test
  (:require [clojure.test :refer :all]
            [org.zsxf.util :as util]))

(deftest keep-every-nth-test-1
  (is
    (=
      (into [] (util/keep-every-nth 10) (range 100))
      [0 10 20 30 40 50 60 70 80 90])))


(deftest all-identical-test-1
  ;WARNING: while _highly_ unlikely to change,
  ; the assertions below rely on Clojure implementation details

  (is (true? (util/all-identical? 1 1 1 1)))
  (is (true? (util/all-identical? {} {})))
  (is (true? (util/all-identical? {} {} {})))
  (is (true? (util/all-identical? {} {} {} {})))
  (is (true? (util/all-identical? {} {} {} {} {})))

  (is (true? (util/all-identical? [] [])))
  (is (true? (util/all-identical? [] [] [] [])))
  (is (true? (util/all-identical? [] [] [] [] [])))

  (is (true? (util/all-identical? #{} #{})))
  (is (true? (util/all-identical? #{} #{} #{})))
  (is (true? (util/all-identical? #{} #{} #{} #{})))

  (is (false? (util/all-identical? 1 2 3))))

(deftest fn-with-source-test-1
  (let [expected-source '(clojure.core/fn [a b c] (+ a b c 42))
        f               (util/fn+ [a b c] (+ a b c 42))]
    ;check source
    (is (= expected-source (util/source f)))
    ;check fn works
    (is (= 45 (f 1 1 1)))))
