(ns org.zsxf.core
  (:require [clojure.string :as str]
            [net.cgrand.xforms :as xforms]
            [nrepl.server :as nrepl-server]
            [org.zsxf.zset :as zs]
            [taoensso.timbre :as timbre]
            [taoensso.encore :as enc]))

; Basic implementation of the ideas presented in
; DBSP: Automatic Incremental View Maintenance for Rich Query Languages
; https://www.youtube.com/watch?v=J4uqlG1mtbU

(defn timbre-custom-output-fn
  "Default (fn [data]) -> final output string, used to produce
  final formatted output_ string from final log data.

  Options (included as `:output-opts` in data sent to fns below):

    :error-fn ; When present and (:?err data) present,
              ; (error-fn data) will be called to generate output
              ; (e.g. a stacktrace) for the error.
              ;
              ; Default value: `default-output-error-fn`.
              ; Use `nil` value to exclude error output.

    :msg-fn   ; When present, (msg-fn data) will be called to
              ; generate a message from `vargs` (vector of raw
              ; logging arguments).
              ;
              ; Default value: `default-output-msg-fn`.
              ; Use `nil` value to exclude message output."

  ([base-output-opts data] ; Back compatibility (before :output-opts)
   (let [data
         (if (empty? base-output-opts)
           data
           (assoc data :output-opts
             (conj
               base-output-opts ; Opts from partial
               (get data :output-opts) ; Opts from data override
               )))]
     (timbre-custom-output-fn data)))
  ([data]
   (let [{:keys [level ?err #_vargs msg_ ?ns-str ?file hostname_
                 timestamp_ ?line output-opts]}
         data]
     (str
       ;timestamp
       ;(when-let [ts (force timestamp_)] (str ts " "))
       ;timbre level
       ;(str/upper-case (name level))  " "
       "[" (or ?ns-str ?file "?") ":" (or ?line "?") "] - "

       (when-let [msg-fn (get output-opts :msg-fn timbre/default-output-msg-fn)]
         (msg-fn data))

       (when-let [err ?err]
         (when-let [ef (get output-opts :error-fn timbre/default-output-error-fn)]
           (when-not   (get output-opts :no-stacktrace?) ; Back compatibility
             (str enc/system-newline
               (ef data)))))))))

(defn -main
  [& args]
  (let [port 7899]
    (timbre/merge-config!
      {:output-fn timbre-custom-output-fn})
    (nrepl-server/start-server :port port)
    (println "ZSXF: REPL on port" port)))

(defn stream->stream
  [s]
  (sequence
    (map +)
    [1 2 3]))

(defn streams->stream
  ""
  [& ss]
  (apply sequence
    (map +)
    ss))

(defn delay-xf
  "Delay the input by one item"
  []
  (fn [rf]
    (let [prev-input (atom nil)]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result input]
         (let [prev-input' @prev-input]
           (reset! prev-input input)
           (if (some? prev-input')
             (rf result prev-input')
             (rf result 0))))))))

(defn changes
  "Returns a stream of changes in a stream of numbers"
  [s]
  (sequence
    (map -)
    s
    (sequence (delay-xf) s)))

(defn changes-xf []
  (xforms/window 2
    (fn
      ([] 0)
      ([x] x)
      ([x y]
       (- y x)))
    +))

(defn integration-xf
  ([] (integration-xf zs/zset+))
  ([f]
   (comp
     (xforms/reductions f)
     (drop 1))))

(defn changes-2
  [s]
  (sequence
    (changes-xf)
    s))

(defn integration
  [s]
  (sequence
     (integration-xf)
    s))
