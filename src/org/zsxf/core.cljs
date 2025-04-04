(ns org.zsxf.core
  (:require [org.zsxf.zset :as zs]
            [taoensso.timbre :as timbre]))


(defn init []
  (println "Hello ClojureScript!")
  (timbre/spy (zs/zset [[1] [2] [3]])))

(init)
