(ns org.zsxf.alpha.repl
  (:require [clj-memory-meter.core :as mm]
            [org.zsxf.query :as q]
            [org.zsxf.util :as util]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as timbre]))


(defn sample-indices
  [query & {:keys [n vfn clause clause-f print-stats]
            :or   {n 2 vfn identity}}]
  (transduce
    (comp
      (filter (fn [[k v]]
                (when print-stats
                  (timbre/info
                    k
                    "original mm:"
                    (mm/measure v)))
                (and (vector? k) (uuid? (second k)))))
      (if clause
        (filter (fn [[k v]] (= clause k)))
        (map identity))
      (if clause-f
        (filter (fn [[k v]] (clause-f k)))
        (map identity))
      (map (fn [[k v]]
             (when print-stats
               (let [;hypothetical (into (empty v) (map (fn [[k v]] [k (vfn v)])) v)
                     ba          (util/time-f (nippy/freeze v) #(timbre/info "freeze time" %))
                     _           (util/time-f (nippy/thaw ba) #(timbre/info "thaw time" %))
                     frozen-size (mm/measure ba)]
                 (timbre/info
                   k "original count:" (count v))
                 ;(timbre/info
                 ;  k "hypothetical mm:" (mm/measure hypothetical))
                 ;(timbre/info
                 ;  k "hypothetical count:" (count hypothetical))
                 (timbre/info
                   k "frozen size" frozen-size)))

             [k (into (empty v)
                  (comp
                    (take n)
                    (map (fn [[k v]]
                           ;(timbre/info "v cnt" (count v))
                           ;(timbre/info "type:" (type (vfn v)))
                           [k (vfn v)])))
                  v)])))
    conj
    {}
    @(::q/state query)))


(comment
  (set! *print-meta* false)

  ((comp type ffirst second first)
   (sample-indices org.zsxf.query-test/query
     :clause '[[?m :movie/cast ?a] #uuid"ff8de2cc-8e1b-42fb-9052-690e513875f9"]
     :print-stats true))

  (mm/measure org.zsxf.test-data.movielens/iron-man-sm)

  (sample-indices org.zsxf.test-data.movielens/iron-man-sm
    :n 5
    :clause-f (fn [k] (= '[?actor :actor/name ?a-name] (first k)))
    :print-stats true
    ;:vfn first
    )

  (mm/measure org.zsxf.test-data.movielens/iron-man-lg)

  (sample-indices org.zsxf.test-data.movielens/iron-man-sm
    :n 3
    :print-stats true)

  (sample-indices org.zsxf.test-data.movielens/iron-man-lg
    :n 3
    :print-stats true)

  (sample-indices org.zsxf.test-data.movielens/iron-man-3
    :n 3
    :print-stats true)

  (sample-indices org.zsxf.test-data.movielens/iron-man-lg
    :n 1
    :clause-f (fn [k]
                (or
                  ;(= '[?actor :actor/name ?a-name] (first k))
                  (= '[?m :movie/cast ?actor] (first k))
                  ))
    :print-stats true)

  (set! *print-meta* true)
  (sample-indices @org.saberstack.datomic.import/*query
    :n 50)
  )
