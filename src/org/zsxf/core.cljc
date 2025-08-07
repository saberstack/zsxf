(ns org.zsxf.core
  (:require
   #?(:clj [nrepl.server :as nrepl-server])
   [org.zsxf.util :as util]
   [taoensso.timbre :as timbre]
   [org.saberstack.server.aleph :as server]))

#?(:clj
   (defn -main
     [& args]
     (let [port 7899]
       (timbre/merge-config!
         {:output-fn util/timbre-custom-output-fn})
       (server/start-server!)
       (nrepl-server/start-server :port port :bind "0.0.0.0")
       (println "ZSXF: REPL on port" port))))

#?(:cljs
   (defn -main
     [& args]
     (println "Hello ClojureScript! ...")))
