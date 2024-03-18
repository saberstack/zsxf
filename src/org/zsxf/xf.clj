(ns org.zsxf.xf
  (:require [net.cgrand.xforms :as xforms]
            [taoensso.timbre :as timbre]))

(defn zset-w [m]
  (timbre/spy
    (:zset/w (meta m) 0)))

(defn ->where-xf
  "SQL WHERE (aka filter), as per https://www.feldera.com/blog/SQL-on-Zsets/#filtering-sql-where"
  [pred]
  (filter pred))

(defn ->dbsp-result-xf!
  "Save DBSP result"
  [an-atom]
  (map (fn [dbsp-result]
         (timbre/spy dbsp-result)
         (reset! an-atom dbsp-result))))

(defn count-xf
  "DBSP SQL COUNT"
  ([rf]
   (let [n (atom 0)]
     (fn
       ([] (rf))
       ([acc] (rf (unreduced (rf acc @n))))
       ([acc item]
        (rf acc (swap! n (fn [prev-n] (+ prev-n (zset-w item))))) acc)))))

(defn for-xf [a-set]
  (xforms/for
    [x % y a-set] [x y]))
