(ns org.zsxf.zset
  (:require [org.zsxf.zset :as-alias zset]
            [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.xf :as dbsp-xf]
            [clojure.math.combinatorics :as comb]
            [taoensso.timbre :as timbre]))

;How to turn a zset into a proper Clojure collection
; Follow the example from https://github.com/clj-commons/ordered/blob/master/src/flatland/ordered/set.clj

(set! *print-meta* true)

(s/def ::zset (s/and
                (s/coll-of map?)                            ;potentially relax this to other collections
                (s/every (fn [set-item-map]
                           (some? (:zset/w (meta set-item-map)))))))

(defn zset? [x]
  (let [result (s/valid? ::zset x)]
    (when-not result (timbre/error (s/explain-data ::zset x)))
    result))

(defn common-keep-and-remove
  [zset-1 zset-2 commons]
  (transduce
    (comp
      (map (fn [common]
             (timbre/spy common)
             (let [meta-1 (meta (zset-1 common))
                   meta-2 (meta (zset-2 common))]
               (with-meta common (merge-with + meta-1 meta-2)))))
      ;group items in two buckets: "remove" and "keep" based on weight
      (xforms/by-key
        (fn [common] (if (zero? (:zset/w (meta common)))
                       :common-remove
                       :common-keep))
        (fn [common] common)
        (fn [remove-or-keep commons] [remove-or-keep commons])
        (xforms/into #{})))
    conj
    {}
    commons))

(defn plus
  "Z-Sets addition implemented as per https://www.feldera.com/blog/Z-sets/#z-sets"
  [zset-1 zset-2]
  {:pre [(zset? zset-1)
         (zset? zset-2)]}
  (let [commons (clojure.set/intersection zset-1 zset-2)
        {:keys [common-keep common-remove]} (common-keep-and-remove zset-1 zset-2 commons)]
    (transduce
      ;get set items one by one
      (comp cat)
      (completing
        (fn [accum set-item]
          (if (zero? (:zset/w (meta set-item)))
            ;remove common-remove items
            (disj accum set-item)
            ;keep everything else
            (conj accum set-item))))
      #{}
      [common-keep
       zset-1
       zset-2
       ;common-remove items **must** stay at the end here to be properly removed during the reduction!
       common-remove])))

(defn negate
  "Change the sign of all the weights in a zset"
  [zset]
  {:pre [(zset? zset)]}
  (transduce
    (map (fn [item]
           (vary-meta item
             (fn [prev-meta]
               (update prev-meta :zset/w (fn [prev-w] (* -1 prev-w)))))))
    conj
    #{}
    zset))

(defn zset-w [m]
  (timbre/spy
    (:zset/w (meta m) 0)))

(defn ->zset
  "Collection to zset as per https://www.feldera.com/blog/Implementing%20Z-sets/#converting-a-collection-to-a-z-set"
  ([coll]
   (->zset coll (map identity)))
  ([coll xf]
   (->zset coll xf 1))
  ([coll xf weight]
   {:pre [(s/valid? (s/coll-of map?) coll)]}
   (transduce
     xf
     (fn
       ([accum]
        (timbre/spy accum))
       ([accum m]
        (timbre/spy accum)
        (timbre/spy m)
        (if-not (coll? m)
          m
          (if-let [existing-m (accum m)]
            (let [accum'      (disj accum existing-m)
                  existing-m' (vary-meta existing-m
                                (fn [prev-meta]
                                  (update prev-meta :zset/w (fn [prev-w] (+ prev-w (zset-w m))))))
                  _           (timbre/spy existing-m')
                  zset-w'     (zset-w existing-m')]
              (if (zero? zset-w')
                accum'
                (conj accum' existing-m')))
            (conj accum (with-meta m {:zset/w weight}))))))
     #{}
     coll)))

(defn ->zset-neg
  [coll]
  (->zset coll (map identity) -1))

(defn cartesian-product [set-1 set-2]
  (set (for [x set-1, y set-2]
         [x y])))

;SELECT * FROM users WHERE status = active;
;JOIN
;GROUP-By
;

(comment
  (let [zset (->zset [{:name "Alice" :age 940} {:name "Bob" :age 950} {:name "Bob" :age 950}])]
    (->zset
      zset
      (dbsp-xf/->where-xf (fn [m] (< 900 (:age m)))))))

(comment
  (let [zset (->zset [{:name "Alice" :age 940} {:name "Alice" :age 941} {:name "Bob" :age 950} {:name "Bob" :age 950}])]
    (->zset
      zset
      (dbsp-xf/->select-xf (fn [m] (select-keys m [:name])))))

  (let [zset (->zset [{:name "Alice" :age 940} {:name "Alice" :age 941} {:name "Bob" :age 950} {:name "Bob" :age 950}])]
    (->zset
      zset
      (dbsp-xf/->select-xf (fn [m] m)))))

(defn ->dbsp-chan-1
  "First working DBSP chan with transducer"
  [xf an-atom]
  (a/chan (a/sliding-buffer 1)
    (comp
      xf
      (dbsp-xf/->dbsp-result-xf! an-atom))))

(def xf-1
  (comp
    (dbsp-xf/->where-xf (fn [m] (< 949 (:age m))))
    dbsp-xf/count-xf))

(defn dbsp-example-1
  "First working example
  INSERT working state"
  []
  (let [an-atom (atom nil)
        ch-1    (->dbsp-chan-1 xf-1 an-atom)]
    (run!
      (fn [m]
        (timbre/spy m)
        (a/>!! ch-1 m))
      (->zset
        [{:name "A" :age 960} {:name "B" :age 961} {:name "C" :age 962}]))

    (run!
      (fn [m]
        (timbre/spy m)
        (a/>!! ch-1 m))
      (->zset
        [{:name "E" :age 900} {:name "F" :age 850} {:name "G" :age 888}]))
    ;*dbsp-result is 3
    (a/close! ch-1)
    @an-atom))


(comment
  (->zset [{:a 1} {:b 2}])                                  ;ok
  (->zset [{:a 1} 1])                                       ;error
  )

(comment
  (plus
    #{^#:zset {:w 2} {:name "Alice"}
      ^#:zset {:w -5} {:name "Bob"}
      ^#:zset {:w -10} {:name "Clara"}}

    #{^#:zset {:w -4} {:name "Alice"}
      ^#:zset {:w 5} {:name "Bob"}
      ^#:zset {:w 1} {:name "Clara"}})

  (plus
    #{^#:zset {:w 2} {:name "Alice"}}

    #{^#:zset {:w -1} {:name "Bob"}})

  (plus
    #{^#:zset {:w 1} {:name "Alice"}}

    #{^#:zset {:w -1} {:name "Alice"}})

  (plus
    #{^#:zset {:w 2} {:name "Alice"}}

    #{^#:zset {:w 0} {:name "Bob"}})


  (negate #{^#:zset {:w 1} {:name "Alice"}})

  (negate #{^#:zset {:w -1} {:name "Bob"}}))

;Next
; https://www.feldera.com/blog/Implementing%20Z-sets/
; https://www.feldera.com/blog/SQL-on-Zsets/
; Use core.async to achieve incremental computation

;Misc
; Calcite functionality that (potentially) supports incremental view maintenance
; https://youtu.be/iT4k5DCnvPU?t=890
; https://calcite.apache.org/javadocAggregate/org/apache/calcite/rel/stream/Delta.html (differentiation)

;; Incremental computation via core.async

(defonce *computation-state (atom {}))

(defn register-computation! [xf atom]

  (a/chan xf))

(defn insert->zset [& maps]
  (->zset `#{~@maps}))

(defn delete->zset [& maps]
  (->zset `#{~@maps}))

(defn for-chan-test []
  (let [ch (a/chan 42
             (comp
               (mapcat identity)
               (dbsp-xf/for-xf #{:a :b})
               (dbsp-xf/->dbsp-result-xf! *computation-state)))]
    (def for-chan-1 ch)))
