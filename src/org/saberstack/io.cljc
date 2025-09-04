(ns org.saberstack.io
  (:require [taoensso.timbre :as timbre]))

(defmacro try-io! [& exprs]
  `(try
     (do ~@exprs)
     (catch Throwable e#
       (let [ex# (ex-info "IO failed" {:code-executed '(do ~@exprs)} e#)]
         (timbre/warn ex#)
         (throw ex#)))))
