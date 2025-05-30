(ns org.zsxf.zset-test
  (:require
   [clojure.spec.alpha :as s]
   [clojure.test.check :as check]
   [clojure.test.check.properties :as prop]
   [medley.core :as medley]
   [org.zsxf.constant :as const]
   [org.zsxf.util :as util]
   [org.zsxf.zset :as zs]
   [clojure.test :refer :all]
   [taoensso.timbre :as timbre]))

(deftest test-1-indexed-zset-pos+
  (let [indexed-zset
        (zs/indexed-zset-pos+
          {1 (zs/zset [[1]])}
          {1 (zs/zset [[1]])})]
    (is (= indexed-zset {1 #{^#:zset{:w 2} [1]}}))))

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

(defn faulty-set->fixed-set [faulty-set]
  (into #{} faulty-set))

(defn generate-zsets
  "Generates a vector of zsets with random but valid zset items based on a spec"
  []
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

(def property-no-zero-weights-zset+
  (prop/for-all [zset-1 (s/gen ::zs/zset)
                 zset-2 (s/gen ::zs/zset)
                 zset-3 (s/gen ::zs/zset)]
    ;we must fix the faulty sets or the results will be all off!
    (let [zset-1            (faulty-set->fixed-set zset-1)
          zset-2            (faulty-set->fixed-set zset-2)
          zset-3            (faulty-set->fixed-set zset-3)
          zset-result       (transduce (map identity) zs/zset+ [zset-1 zset-2 zset-3])
          zset-item-weights (mapv (fn [zset-item] (zs/zset-weight zset-item)) zset-result)]
      ;expect no zero weights
      (is (nil? (medley/find-first zero? zset-item-weights)))
      ;expect #{} when we zset+ a zset with the negation of itself
      (is (= #{} (zs/zset+ zset-1 (zs/zset-negate zset-1))))
      (is (= #{} (zs/zset+ zset-2 (zs/zset-negate zset-2))))
      (is (= #{} (zs/zset+ zset-3 (zs/zset-negate zset-3)))))))

(deftest no-zero-weights-after-zset+
  (check/quick-check 100 property-no-zero-weights-zset+))

;(deftest maybe-zsi-disj
;  (let [a-zset (zs/zset+
;                 (map identity)
;                 (zs/zset+
;                   #{(rel/mark-as-opt-rel
;                       (zs/zset-item [:a :b]))})
;                 #{(rel/mark-as-opt-rel
;                     (zs/zset-item [:a [:not-found]]))}
;                 #{(rel/mark-as-opt-rel
;                     (zs/zset-item [:a [:not-found]]))}
;                 #{(rel/mark-as-opt-rel
;                     (zs/zset-item [:a :b] -1))})]
;    (is (= #{} a-zset))
;    (is (= #{} (zs/zset-denied-not-found a-zset)))))
