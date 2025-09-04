(ns org.saberstack.datasync.demo.live
  (:require
   [clj-commons.byte-streams :as bs]
   [org.saberstack.datasync.datomic.import :as import]
   [org.saberstack.datasync.hn :as hn]
   [clojure.string :as str]
   [clojure.core.match :as match]
   [org.saberstack.util.transit :as util.transit]
   [org.saberstack.io :as ss.io]
   [ring.util.codec :as ring-codec]
   [taoensso.timbre :as timbre]))


(defn transit-response [resp]
  ;(info transit-response-f)
  (-> resp
    (update-in [:body] #(util.transit/data-to-transit %))
    (update-in [:headers] (fn [headers] (assoc headers "content-type" "application/transit+json")))))

(defn transit-request-f [req]
  (timbre/spy req)
  (-> req
    (update-in [:body] (fnil bs/to-string "{}"))
    (update-in [:body] #(util.transit/transit-to-data %))))

(defn queries [req]
  (transit-response
    {:status 200
     :body   (into []
               (comp (map meta) (map (fn [m] (select-keys m [:doc :name]))))
               [#'import/get-all-clojure-mentions-by-raspasov
                #'import/get-all-clojure-mentions-user-count])}))

(defonce tmp-sym-1 (atom nil))

(defn query-name->fn [a-name]
  (timbre/spy a-name)
  (let [sym (symbol (str 'org.saberstack.datasync.datomic.import) (str a-name))]
    (timbre/info "sym:::" sym)
    (reset! tmp-sym-1 sym)
    (condp = sym
      `import/get-all-clojure-mentions-by-raspasov 'get-all-clojure-mentions-by-raspasov
      `import/get-all-clojure-mentions-user-count 'get-all-clojure-mentions-user-count)))

(defn query-result [req a-name]
  (timbre/spy (type a-name))
  (transit-response
    {:status 200
     :body   {:get-result (str(query-name->fn a-name))}}))

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
      [:get ["query" a-name "result"]] (query-result req a-name)

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
