(ns org.saberstack.server.aleph
  (:require [aleph.http :as aleph]
            [clj-commons.byte-streams :as bs]
            [aleph.netty]
            [config.core :as config]
            [ring.middleware.defaults :as ring-defaults]
            [ring.middleware.params :as ring-params]
            [org.saberstack.demo.live :as demo.live]
            [taoensso.timbre :as timbre])
  (:import (java.io Closeable)))

(defonce server-state (atom nil))

(defn env-vars->ssl-context
  [{:keys [tls-cert tls-private-key] :as _env-map}]
  (when (and tls-cert tls-private-key)
    {:ssl-context
     (aleph.netty/ssl-server-context
       {:certificate-chain (bs/to-input-stream (slurp tls-cert))
        :private-key       (bs/to-input-stream (slurp tls-private-key))})}))

(defn http-req-dispatch
  [{:keys [uri request-method] :as req}]
  {:status  200
   :headers {"content-type" "text/plain"}
   :body    "hello"})

(defn resp-error [_req ^Throwable e]
  {:status 500
   :body   (str (ex-data e))})

(defn handler-2
  "Indirections for REPL-friendly development."
  [req]
  (timbre/info req)
  (try
    (http-req-dispatch req)
    (catch Throwable e (do (timbre/error e) (resp-error req e)))))

(defn handler
  [req]
  (handler-2 req))

(defn start-server! [& {:keys [port socket-address] :or {socket-address "0.0.0.0"}}]
  (reset! server-state
    (let [ssl-context (env-vars->ssl-context config/env)
          port (if ssl-context (or port 443) (or port 8042))]
      (aleph/start-server
        (-> demo.live/handler
          (ring-defaults/wrap-defaults ring-defaults/api-defaults)
          (ring-defaults/wrap-defaults
            (assoc-in ring-defaults/api-defaults [:session :cookie-attrs :same-site] :lax))
          (ring-params/wrap-params))
        (merge
          {:port          port
           :http-versions [:http1]}
          ssl-context)))))

(defn stop-server! []
  (.close ^Closeable @server-state))

(comment
  (do
    (stop-server!)
    (start-server!)))
