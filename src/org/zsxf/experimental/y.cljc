(ns org.zsxf.experimental.y)

(defn Y
  [gen-f]
  (let [g (fn [x] (gen-f (fn [n k] #((x x) n k))))]
    (fn [n k]
      (trampoline #((g g) n k)))))

(def sum-seq-cps
  (Y
    (fn [r]
      (fn [s k]
        (if (empty? s)
          (fn [] (k 0))
          (fn []
            (r
              (rest s)
              (fn [v]
                (fn [] (k (+ (first s) v)))))))))))

(defonce r (atom nil))
(comment
  (do
    (reset! r (into [] (range 100000000)))
    :done)

  ;No stackoverflow, but unfortunately...
  ;...it is pretty slow and likely uses a great deal of memory
  ; due to continuous fn/object allocation during execution.
  ; discussion: https://groups.google.com/g/clojure/c/YlfLxEVLy4U/m/0DPgDqjgJ2IJ
  (time (sum-seq-cps @r identity)))

(defn xf-sum-all
  "Transducer manually adjusted to be tail recursive.
  Preventing stackoverflow via recur."
  ([accum v]
   (let [final-accum
         (transduce
           (comp
             (map-indexed
               (fn [idx num-or-v]
                 [idx num-or-v]))
             (halt-when
               (comp vector? second)
               (fn [accum [idx def-v]]
                 {:halt [accum [idx def-v]]}))
             (map (fn [[_idx def-num]]
                    def-num)))
           +
           accum
           v)]
     (if-let [[halt-accum [_idx v-cause]] (:halt final-accum)]
       (recur halt-accum v-cause)
       final-accum))))

(defn- make-multi-v
  "Make a big nested vector"
  [n]
  (transduce
    (map (fn [v] v))
    (completing
      (fn
        ([] [1 1 1])
        ([accum v]
         (conj v accum))))
    (repeat
      n
      [111])))

(comment
  (xf-sum-all 0 [1 1 1 1 [1 1 1 [1 1 1]]])
  (xf-sum-all 0 (make-multi-v 100000000)))
