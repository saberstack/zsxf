(ns org.saberstack.datasync.demo.live
  (:require
   [org.saberstack.datomic.import :as import]
   [org.saberstack.datasync.hn :as hn]
   [clojure.string :as str]
   [clojure.core.match :as match]
   [org.saberstack.io :as ss.io]
   [ring.util.codec :as ring-codec]
   [taoensso.timbre :as timbre]))

(defn queries [req]
  {:status  200
   :headers {"content-type" "text/plain"}
   :body    "queries"})

(defn status
  [_req]
  {:status  200
   :headers {"content-type" "text/plain"}
   :body    "ok"})

(defn resp-not-found [_req]
  {:status  404
   :headers {"content-type" "text/plain"}
   :body    "404"})

(defn resp-error [_req ^Throwable e]
  (let [error-data (apply conj []
                     (if-let [data (ex-data e)] (str data) (str e))
                     (map str (.getStackTrace e)))]
    {:status  500
     :headers {"content-type" "text/plain"}
     :body    (str/join "\n" error-data)}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Request dispatch and routing, start section
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn http-req-dispatch
  [{:keys [uri request-method] :as req}]
  (let [uri-as-vector         (-> uri (str/replace-first #"\/" "") (str/split #"\/"))
        uri-as-vector-decoded (mapv ring-codec/url-decode uri-as-vector)]
    (match/match [request-method uri-as-vector-decoded]
      [:get ["queries"]] (queries req)

      [:get ["status"]] (status req)
      :else (resp-not-found req))))

(defn handler*
  "Indirections for REPL-friendly development. Please do not remove."
  [req]
  (timbre/info req)
  (try
    (http-req-dispatch req)
    (catch Throwable e (do (timbre/error e) (resp-error req e)))))

(defn handler
  [req]
  (handler* req))
