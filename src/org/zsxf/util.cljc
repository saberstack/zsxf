(ns org.zsxf.util
  (:require [datascript.core :as d]
            [taoensso.timbre :as timbre]
            #?(:clj [clojure.edn :as edn])
            #?(:clj [clojure.java.io :as io])
            #?(:clj [taoensso.encore :as enc]))
  #?(:clj
     (:import (clojure.lang IObj)
              (java.io PushbackReader))))

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
   ((fpred nth (comp not indexed?) nil) coll index not-found)))

(defn peekv [coll]
  ((fpred peek (comp not vector?) nil) coll))

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
         ret#   ~expr
         time#  (/ (double (- (. System (nanoTime)) start#)) 1000000.0)]
     (~f time#)
     ret#))

(defn keep-every-nth
  "Transducer that keeps every nth item."
  [n]
  (keep-indexed
    (fn [index item]
      (when (int? (/ index n))
        item))))

(defn can-meta?
  "Check if x can have metadata."
  [x]
  #?(:clj
     (instance? IObj x)
     :cljs
     (implements? IWithMeta x)))

(comment
  ;usage
  (into [] (keep-every-nth 10) (range 100))
  ;=> [0 10 20 30 40 50 60 70 80 90]
  )


(defn >inst [inst-1 inst-2]
  (condp = (compare inst-1 inst-2)
    0 false
    -1 false
    1 true))

(defn path-f
  "Create a path function.
  Takes a vector of indices.
  Returns a function that takes an indexed collection and returns the value at the path."
  [v]
  (if (empty? v)
    identity
    (transduce
      (map (fn [idx]
             (fn [x]
               (cond
                 (int? idx) (nth2 x idx)
                 (fn? idx) (idx x)
                 :else (throw (ex-info "path components must satisfy either int? or fn?" {:given x}))))))
      (completing
        conj
        (fn [accum]
          (apply comp accum)))
      ;keep this as '() to preserve `get-in`-style order of indices
      '()
      v)))

(comment
  (macroexpand-1 '(path-f []))
  (macroexpand-1 '(path-f [1 2 3])))

(defn sfirst [x]
  (second (first x)))

#?(:clj
   (defn read-edn-file [file-path]
     (with-open [r (io/reader file-path)]
       (edn/read (PushbackReader. r)))))

#?(:clj
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
             (list name (into ['this] (take argcount (repeatedly gensym))))))))))

#?(:clj
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

     ([base-output-opts data]                               ; Back compatibility (before :output-opts)
      (let [data
            (if (empty? base-output-opts)
              data
              (assoc data :output-opts
                (conj
                  base-output-opts                          ; Opts from partial
                  (get data :output-opts)                   ; Opts from data override
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
              (when-not (get output-opts :no-stacktrace?)   ; Back compatibility
                (str enc/system-newline
                  (ef data))))))))))

#?(:clj
   (defn load-learn-db
     []
     (let [schema (read-edn-file "resources/learndatalogtoday/schema_datascript.edn")
           data   (read-edn-file "resources/learndatalogtoday/data_datascript.edn")
           conn   (d/create-conn schema)
           _      (d/transact! conn data)]
       [conn schema])))

#?(:clj
   (defn load-learn-db-empty
     []
     (let [schema (read-edn-file "resources/learndatalogtoday/schema_datascript.edn")
           conn   (d/create-conn schema)]
       [conn schema])))

#?(:clj
   (defn jvm-version []
     (parse-long (System/getProperty "java.vm.specification.version"))))

(defn megabytes [num-of-bytes]
  (double
    (/ num-of-bytes 1000000)))

(defn datom-like?
  "A helper fn to allow working with vectors instead of datoms"
  [x]
  (boolean
    (and (nth2 x 0) (nth2 x 1) (nth2 x 2)
      (int? (nth2 x 0))
      (keyword? (nth2 x 1)))))

(defn ?vary-meta [obj f & args]
  (if (can-meta? obj)
    (apply vary-meta obj f args)
    obj))

(defn all-identical?
  "Like clojure.core/identical? but supports multiple items, similar to clojure.core/="
  ([x y]
   (identical? x y))
  ([x y & more]
   (if (identical? x y)
     (if (next more)
       (recur y (first more) (next more))
       (identical? y (first more)))
     false)))

(defmacro fn+
  "Same as clojure.core/fn but also attach the source to the function as metadata"
  [& body]
  `(with-meta (fn ~@body) {:source '(fn ~@body)}))

(defn source
  "Return the source as data of a fn created via fn+"
  [f]
  (:source (meta f)))


(comment

  (let [f (fn+ [a b c] (+ a b c 42))]
    (source f))

  (scaffold clojure.lang.IPersistentMap)

  (scaffold clojure.lang.IPersistentSet)

  (scaffold clojure.lang.APersistentSet)
  (scaffold clojure.lang.PersistentHashSet)

  (scaffold clojure.lang.IPersistentCollection)

  (scaffold clojure.lang.IPersistentVector))
