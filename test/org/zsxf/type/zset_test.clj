(ns org.zsxf.type.zset-test
  (:require
   [clojure.test :refer [deftest is]]
   [org.zsxf.type.zset :as zs2]
   [clojure.spec.alpha :as s]
   [clojure.test.check :as check]
   [clojure.test.check.properties :as prop]
   [medley.core :as medley]
   [org.zsxf.constant :as const]
   [org.zsxf.util :as util]
   [org.zsxf.zset :as-alias zs]
   [taoensso.timbre :as timbre])
  (:import (clojure.lang ITransientCollection)))

(defn set-debug [s value]
  (set! *print-meta* true)
  {:s-value (s value)
   :first-s (first s)})

(defn meta=
  "Equality that also compares metadata.
  Returns true only if all the values are = and their metadata maps are =."
  ([x y]
   (and
     (= x y)
     (= (meta x) (meta y))))
  ([x y & more]
   (if (meta= x y)
     (if (next more)
       (recur y (first more) (next more))
       (meta= y (first more)))
     false)))

(deftest zset*-unit-test-1
  (let [z          (zs2/zset*
                     (zs2/zset #{(zs2/zset-item {:a 41} 2) (zs2/zset-item {:a 42} 2)})
                     (zs2/zset #{(zs2/zset-item {:a 41} 2) (zs2/zset-item {:a 42} 2)}))
        z-expected #zs #{^#:zset {:w 4} [{:a 41} {:a 42}]
                         ^#:zset {:w 4} [{:a 41} {:a 41}]
                         ^#:zset {:w 4} [{:a 42} {:a 42}]
                         ^#:zset {:w 4} [{:a 42} {:a 41}]}]
    (= z-expected z)))



(deftest zset-transient-match-regular
  (let [v              ["42"]
        v-meta-1       (with-meta ["42"] {:zset/w 1 :meta "a"})
        v-meta-2       (with-meta ["42"] {:zset/w 1 :meta "b"})
        zset-transient (->
                         (zs2/zset)
                         (transient)
                         (conj! v-meta-1)
                         (conj! v-meta-2)
                         (persistent!))
        zset-regular   (->
                         (zs2/zset)
                         (conj v-meta-1)
                         (conj v-meta-2))
        items          (into []
                         (comp
                           (map vals)
                           cat)
                         [(set-debug zset-transient v)
                          (set-debug zset-transient v-meta-1)
                          (set-debug zset-transient v-meta-2)
                          (set-debug zset-regular v)
                          (set-debug zset-regular v-meta-1)
                          (set-debug zset-regular v-meta-2)])]
    (is (true? (apply meta= items)))))

(deftest zset-equal-to-clojure-set
  (let [s   #{[1] [2]}
        zs  #zs #{[1] [2]}
        zsp #zsp #{[1] [2]}]
    (is (= s zs zsp))
    (is (= zs s zsp))
    (is (= zsp zs s))))

(deftest zset-equal-to-clojure-set-with-coll-items
  (let [s   #{[1] [2]}
        zs  #zs #{[1] [2]}
        zsp #zsp #{[1] [2]}]
    (is (= s zs zsp))
    (is (= zs s zsp))
    (is (= zsp zs s))))

(deftest test-1-indexed-zset-pos+
  (let [indexed-zset
        (zs2/indexed-zset-pos+
          {1 (zs2/zset [[1]])}
          {1 (zs2/zset [[1]])})]
    (is (= indexed-zset {1 #{^#:zset{:w 2} [1]}}))))

(deftest test-4-zset-count
  (is
    (= (zs2/zset+
         #{(zs2/zset-count-item 42 :counter1)}
         #{(zs2/zset-count-item 42 :counter1)})
      #zs #{(zs2/zset-count-item 84 :counter1)})))

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

(defonce *generated (atom nil))

(defn- -zset
  ([] (zs2/zset))
  ([coll] (zs2/zset coll)))

(deftest generative-test-1-zset+
  (let [{:keys [equal zsets] :as generated} (generate-zsets-with-equal-items)
        _                (reset! *generated generated)
        ;sum manually
        weight-sum       (apply + (map zs2/-zset-weight equal))
        ;zset+ sum
        zset-summed      (transduce (map -zset) zs2/zset+ zsets)
        ;values found in the previous step must be equal, check here
        one-value        (into #{} equal)
        _                (is (= 1 (count one-value)))
        one-value'       (with-meta (first one-value) nil)
        zset-sum-result  (zs2/zset-weight (zset-summed (first one-value)))
        zset-sum-result' ((fnil + 0) zset-sum-result)]
    (timbre/set-min-level! :debug)
    (timbre/spy weight-sum)
    (timbre/spy zset-summed)
    (timbre/spy (zset-summed (first one-value)))
    (timbre/spy one-value')
    (timbre/spy zset-sum-result)
    (timbre/spy zset-sum-result')
    (is (= zset-sum-result' weight-sum))))

(comment
  (mapv zs2/-zset-weight
    (:equal @*generated))

  (transduce (map identity)
    zs2/zset+
    (:zsets @*generated))

  (count (into #{} (:equal @*generated))))

(def property-no-zero-weights-zset+
  (prop/for-all [zset-1 (s/gen ::zs/zset)
                 zset-2 (s/gen ::zs/zset)
                 zset-3 (s/gen ::zs/zset)]
    ;we must fix the faulty sets or the results will be all off!
    (let [zset-1            (faulty-set->fixed-set zset-1)
          zset-2            (faulty-set->fixed-set zset-2)
          zset-3            (faulty-set->fixed-set zset-3)
          zset-result       (transduce (map identity) zs2/zset+ [zset-1 zset-2 zset-3])
          zset-item-weights (mapv (fn [zset-item] (zs2/zset-weight zset-item)) zset-result)]
      ;expect no zero weights
      (is (nil? (medley/find-first zero? zset-item-weights)))
      ;expect #{} when we zset+ a zset with the negation of itself
      (is (= #{} (zs2/zset+ zset-1 (zs2/zset-negate zset-1))))
      (is (= #{} (zs2/zset+ zset-2 (zs2/zset-negate zset-2))))
      (is (= #{} (zs2/zset+ zset-3 (zs2/zset-negate zset-3)))))))

(deftest no-zero-weights-after-zset+
  (check/quick-check 100 property-no-zero-weights-zset+))


(defn gen-123 []
  (prop/for-all [zset-1 (s/gen ::zs/zset)]))
