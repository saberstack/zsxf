(ns org.zsxf.util
  (:require [clojure.core.async :as a]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [taoensso.timbre :as timbre]
            [taoensso.encore :as enc])
  (:import (clojure.lang IReduceInit)
           (java.io PushbackReader)))

(defn reducible->chan
  "Take the rows from the reducible and put them onto a channel. Return the channel.
  Useful for streaming a large number of rows from a database table without out-of-memory errors."
  [^IReduceInit reducible ch]
  (future
    (transduce
      (comp
        (map (fn [row] (a/>!! ch row)))
        ; halt when the receiving channel is full
        ; WARNING: core.async sliding-buffer and dropping-buffer will not halt
        ;(halt-when nil?)
        )
      conj
      []
      (eduction
        (map (fn [row] (into {} row)))
        reducible))
    (a/close! ch))
  ;return channel
  ch)

(defn fpred
  "Like fnil, but with a custom predicate"
  ([f pred x]
   (fn
     ([a] (f (if (pred a) x a)))
     ([a b] (f (if (pred a) x a) b))
     ([a b c] (f (if (pred a) x a) b c))
     ([a b c & ds] (apply f (if (pred a) x a) b c ds))))
  ([f pred x y]
   (fn
     ([a b] (f (if (pred a) x a) (if (pred b) y b)))
     ([a b c] (f (if (pred a) x a) (if (pred b) y b) c))
     ([a b c & ds] (apply f (if (pred a) x a) (if (pred b) y b) c ds))))
  ([f pred x y z]
   (fn
     ([a b] (f (if (pred a) x a) (if (pred b) y b)))
     ([a b c] (f (if (pred a) x a) (if (pred b) y b) (if (pred c) z c)))
     ([a b c & ds] (apply f (if (pred a) x a) (if (pred b) y b) (if (pred c) z c) ds)))))

(defn nth2
  "Like nth but doesn't throw"
  ([coll index]
   (nth2 coll index nil))
  ([coll index not-found]
   ((fpred nth (comp not vector?) nil) coll index not-found)))

(defn key-intersection
  "Taken from clojure.set/intersection but adapted to work for maps.
  Takes maps m1 and m2.
  Returns a set of common keys."
  [m1 m2]
  (if (< (count m2) (count m1))
    (recur m2 m1)
    (reduce
      (fn [result item]
        (if (contains? m2 item)
          (conj result item)
          result))
      #{}
      (keys m1))))

(defmacro with-source [xform]
  `(with-meta
    ~xform
    {:source '~xform}))

(defn timbre-custom-output-fn
  ;Mostly copied from timbre/default-output-fn, changed to remove verbose timestamp
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

(defn load-edn-file [file-path]
  (with-open [rdr (io/reader file-path)]
    (edn/read (PushbackReader. rdr))))

(defn take-lastv
  "Similar to take-last but uses subvec, which is O(1).
  Might return less than n items if n is greater than the count of v.
  Returns a vector via subvec of original vector v."
  [n v]
  (let [cnt (count v)]
    (subvec v (max 0 (- cnt n)) cnt)))


(defmacro time-f
  "Like time but accepts a function to call with the elapsed time."
  [expr f]
  `(let [start# (. System (nanoTime))
         ret# ~expr
         time# (/ (double (- (. System (nanoTime)) start#)) 1000000.0)]
     (~f time#)
     ret#))

(defn scaffold
  "Clojure Scaffolding for deftype.
  Show which methods a class implements and for which interfaces"
  [iface]
  ;; Big thanks to Christophe Grand
  ;; https://groups.google.com/d/msg/clojure/L1GiqSyQVVg/m-WJogaqU8sJ
  (doseq [[iface methods] (->> iface .getMethods
                            (map #(vector (.getName (.getDeclaringClass %))
                                    (symbol (.getName %))
                                    (count (.getParameterTypes %))))
                            (group-by first))]
    (println (str "  " iface))
    (doseq [[_ name argcount] methods]
      (println
        (str "    "
          (list name (into ['this] (take argcount (repeatedly gensym)))))))))

(comment
  (scaffold clojure.lang.IPersistentMap))
