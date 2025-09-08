(ns org.zsxf.core
  #?(:clj
     (:require
      [aleph.netty]
      [nrepl.server :as nrepl-server]
      [org.saberstack.datasync.demo.live :as demo.live]
      [org.zsxf.util :as util]
      [clojure.repl.deps]
      [taoensso.timbre :as timbre]
      [org.saberstack.server.aleph :as server])))

#?(:clj
   (defn -main
     [& args]
     (let [port 7899]
       (timbre/merge-config!
         {:output-fn util/timbre-custom-output-fn})
       (let [server   (server/start-server!)
             args-set (into #{} args)]
         (timbre/info "-main args :::" args)
         (println "Aleph: started on port" (aleph.netty/port server))
         (nrepl-server/start-server :port port :bind "0.0.0.0")
         (println "ZSXF: REPL on port" port)
         (when (contains? args-set "demo.live.start")
           (demo.live/hn-start-live-sync!))))))

#?(:cljs
   (defn -main
     [& args]
     (println "Hello ClojureScript! ...")))
