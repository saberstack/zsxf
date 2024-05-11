(ns org.zsxf.xf
  (:require [net.cgrand.xforms :as xforms]
            [taoensso.timbre :as timbre]))

(defn zset-w [m]
  (timbre/spy
    (:zset/w (meta m) 0)))

(defn ->where-xf
  "SQL WHERE (aka filter), as per https://www.feldera.com/blog/SQL-on-Zsets/#filtering-sql-where
  This is different from regular Clojure filter in that it returns an empty set if the predicate is false.
  DBSP requires all operators (in our case, transducers) to return a zset rather than skipping the item."
  [pred]
  (map (fn [x]
         (if (pred x) x #{}))))

(defn ->dbsp-result-xf!
  "Save DBSP result"
  [an-atom]
  (map (fn [dbsp-result]
         (timbre/spy dbsp-result)
         (reset! an-atom dbsp-result))))

(defn ->index-xf
  [kfn]
  (xforms/by-key
    kfn
    (fn [m] m)
    (fn [k ms]
      (timbre/spy k)
      (timbre/spy ms)
      (if k {k ms} {}))
    (xforms/into #{})))

(defn count-xf
  "DBSP SQL COUNT"
  ([rf]
   (let [n (atom 0)]
     (fn
       ([] (rf))
       ([acc] (rf (unreduced (rf acc @n))))
       ([acc item]
        (timbre/spy item)
        (rf acc (swap! n (fn [prev-n] (+ prev-n (zset-w item))))) acc)))))

(defn for-xf [a-set]
  (xforms/for
    [x % y a-set] [x y]))
