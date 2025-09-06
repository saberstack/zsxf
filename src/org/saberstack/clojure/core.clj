(ns org.saberstack.clojure.core
  "Experimental. Subject to change."
  (:refer-clojure :exclude [time]))

(defn conj-conj!
  "conj! that matches the behavior of conj in terms of handling transients."
  ;TODO measure performance cost
  ([] (transient []))
  ([coll] coll)
  ([^clojure.lang.ITransientCollection coll x]
   (if (contains? coll x)
     coll
     (.conj coll x))))

(defmacro time
  "Like clojure.core/time but accepts many arguments."
  {:added "1.0"}
  [& exprs]
  `(let [start# (. System (nanoTime))
         ret#   (do ~@exprs)]
     (prn (str "Elapsed time: " (/ (double (- (. System (nanoTime)) start#)) 1000000.0) " msecs"))
     ret#))

(defn- add-to-second-position [lst item]
  (let [[start end] (split-at 1 (if (list? lst) lst [lst]))]
    (concat start [item] end)))

(defmacro every->
  [expr & forms]
  (let [g     (gensym)
        steps (map
                (fn [step]
                  `(if (nil? ~g)
                     nil
                     (let [ret# (-> ~g ~step)]
                       (if (nil? ret#)
                         (add-to-second-position '~step ~g)
                         ret#))))
                forms)]
    `(let [~g ~expr
           ~@(interleave (repeat g) (butlast steps))]
       ~(if (empty? steps)
          g
          (last steps)))))

;; https://clojure.atlassian.net/issues/CLJ-1615
;; https://clojurians.slack.com/archives/C03S1KBA2/p1744219939763269
;; conj! with-meta problem description:
(comment
  (let [set-debug (fn [s msg]
                    (set! *print-meta* true)
                    (println msg)
                    (clojure.pprint/pprint
                      {:item-from-set      (s [:a])
                       :first-of-set       (first s)
                       :seq-of-set         (seq s)
                       :meta-of-first-of-s (meta (first s))})
                    (println))
        v1        (with-meta [:a] {:n 0})
        v2        (with-meta [:a] {:n -42})
        ;conj! problem
        s         (-> (transient #{}) (conj! v1) (conj! v2))
        bad-set   (persistent! s)
        ;conj-conj! fix
        s         (-> (transient #{}) (conj-conj! v1) (conj-conj! v2))
        good-set  (persistent! s)]
    (set-debug bad-set "bad set")
    (set-debug good-set "good set, maybe?")))
