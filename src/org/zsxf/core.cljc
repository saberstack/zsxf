(ns org.zsxf.core
  (:require
   #?(:clj [nrepl.server :as nrepl-server])
   [org.zsxf.util :as util]
   [taoensso.timbre :as timbre]))

#?(:clj
   (defn -main
     [& args]
     (let [port 7899]
       (timbre/merge-config!
         {:output-fn util/timbre-custom-output-fn})
       (nrepl-server/start-server :port port)
       (println "ZSXF: REPL on port" port))))

#?(:cljs
   (defn -main
     [& args]
     (println "Hello ClojureScript! ...")))
