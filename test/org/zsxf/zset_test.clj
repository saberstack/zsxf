(ns org.zsxf.zset-test
  (:require
   [clojure.spec.alpha :as s]
   [medley.core :as medley]
   [org.zsxf.zset :as zs]
   [clojure.test :refer :all]
   [taoensso.timbre :as timbre]))

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

(deftest test-4-zset-count
  (is
    (= (zs/zset+
         #{(zs/zset-count-item 42)}
         #{(zs/zset-count-item 42)})
      #{(zs/zset-count-item 84)})))

(defn equal->vec [zsets intersection]
  (transduce
    (comp
      cat
      (filter (fn [item]
                (when (contains? intersection item)
                  item))))
    conj
    zsets))

(defn generate-zsets []
  "Generates a vector of zsets with random but valid zset items based on a spec"
  (transduce
    (comp
      (map first)
      (map (fn [faulty-zset]
             ;this is needed because of
             ;https://clojure.atlassian.net/issues/CLJ-1615
             ; specifically, test.check/spec generators seem to use transients
             ; while creating values and the metadata gets faulty
             ;putting the data
             (into #{} faulty-zset))))
    conj
    (s/exercise ::zs/zset)))

(defn generate-zsets-with-equal-items []
  (let [equal
        (medley/find-first #(not (empty? %))
          ;run into we find zsets with common items
          (repeatedly
            (fn []
              (let [zsets        (generate-zsets)
                    intersection (apply clojure.set/intersection zsets)]
                (if (empty? intersection)
                  intersection
                  {:zsets zsets
                   :equal (equal->vec zsets intersection)})))))]
    equal))

(deftest generative-test-1-zset+
  (let [{:keys [equal zsets]} (generate-zsets-with-equal-items)
        ;sum manually
        weight-sum      (apply + (map zs/zset-weight equal))
        ;zset+ sum
        zset-summed     (transduce (map identity) zs/zset+ zsets)
        ;values found in the previous step must be equal, check here
        equal-value     (into #{} equal)
        _               (is (= 1 (count equal-value)))
        equal-value'    (with-meta (first equal-value) nil)
        zset-sum-result (zs/zset-weight (zset-summed (first equal-value)))]
    (timbre/spy equal-value')
    (timbre/spy zset-sum-result)
    (timbre/spy zset-summed)
    (is (= zset-sum-result weight-sum))))
