(ns org.saberstack.io
  (:require [taoensso.timbre :as timbre]))

(defmacro try-io! [& exprs]
  `(try
     (do ~@exprs)
     (catch Throwable e#
       (timbre/warn
         (ex-info "IO failed" {:code-executed '(do ~@exprs)} e#)))))
