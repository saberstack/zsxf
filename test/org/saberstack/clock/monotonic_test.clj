(ns org.saberstack.clock.monotonic-test
  (:require
   [clojure.test :refer :all]
   [org.saberstack.clock.monotonic :as monotonic]))

(deftest monotonic-test
  (testing "monotonic clock"
    (is
      (true?
        (apply <
          (map second
            (sort-by first
              (pmap
                (fn [_]
                  (monotonic/now+generation))
                (range 1000)))))))))
