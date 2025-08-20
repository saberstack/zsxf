(ns org.zsxf.type.zset-test
  (:require
   [clojure.test :refer [deftest is]]
   [org.zsxf.type.zset :as zs2]))

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
  (let [s   #{1 2}
        zs  #zs #{1 2}
        zsp #zsp #{1 2}]
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
