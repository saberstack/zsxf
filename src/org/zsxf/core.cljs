(ns org.zsxf.core
  (:require [org.zsxf.xf :as xf]
            [org.zsxf.zset :as zs]
            [cljs.pprint]
            [taoensso.timbre :as timbre]))


(defn init []
  (println "Hello ClojureScript! ...")
  (timbre/spy (zs/zset [[1] [2] [3]]))
  xf/join-xf)

(defn -main [& args]
  (init))
