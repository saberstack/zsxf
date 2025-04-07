(ns org.zsxf.datascript
  (:require [org.zsxf.query :as q]
            [org.zsxf.util :as util]
            [org.zsxf.zset :as zs]
            [datascript.core :as d]
            [taoensso.timbre :as timbre]
            #?(:clj [org.zsxf.type :as t]))
  #?(:clj
     (:import (datascript.db Datom)
              (org.zsxf.type Datom2))))

(defn datom2 [datom]
  #?(:clj
     (t/->Datom2 datom nil)
     :cljs
     ;TODO implement Datom2 for CLJS
     (let [[e a v _t add-or-retract :as datom] datom]
       [e a v])))

(defn eligible-datom? [datom]
  #?(:clj
     (instance? Datom2 datom)
     :cljs
     ;TODO implement Datom2 for CLJS
     (vector? datom)))

;; End CLJC code
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn datom->weight [datom]
  (let [weight (condp = (nth datom 4) true 1 false -1)]
    weight))

(defn datom->zset-item [[e a v _t add-or-retract :as datom]]
  (zs/zset-item [e a v] (datom->weight datom)))

(defn datom->datom2->zset-item [datom]
  (zs/zset-item (datom2 datom) (datom->weight datom)))

(defn tx-datoms->zset
  "Transforms datoms into a zset of vectors. Each vector represents a datom with a weight."
  [datoms]
  (transduce
    (map datom->zset-item)
    conj
    #{}
    datoms))

(defn tx-datoms->datoms2->zset
  "Transforms datoms into a zset of vectors. Each vector represents a datom with a weight."
  [datoms]
  (transduce
    (map datom->datom2->zset-item)
    conj
    #{}
    datoms))

(defn tx-datoms->zsets2
  "Transforms datoms into datoms2, and then into a vector of zsets.
  Useful to maintain inter-transaction order of datoms."
  [datoms]
  (into
    []
    (comp
      (map datom->datom2->zset-item)
      (map hash-set))
    datoms))

(defn datom->eid [datom]
  (if (eligible-datom? datom)
    (nth datom 0 nil)))

(defn datom->attr [datom]
  (if (eligible-datom? datom)
    (nth datom 1 nil)))

(defn datom-attr= [datom attr]
  (= (datom->attr datom) attr))

(defn datom->val [datom]
  (if (eligible-datom? datom)
    (nth datom 2 nil)))

(defn datom-val= [datom value]
  (= (datom->val datom) value))

(defn datom-attr-val= [datom attr value]
  (and (datom-attr= datom attr) (datom-val= datom value)))

(defn listen!
  "Initialize a listener for a given query and connection.
  Supports a time-f function that will be called with the time taken to process each transaction.
  Returns true"
  [conn query]
  (d/listen! conn (q/get-id query)
    (fn [tx-report]
      (q/input query (timbre/spy (tx-datoms->zsets2 (:tx-data tx-report))))))
  ;return
  true)

(defn init-query-with-conn
  "Initial naive implementation.
  Assumes no writes are incoming during initialization.
  WIP"
  [query conn & {:keys [time-f] :or {time-f identity}}]
  (let [db      @conn
        ;load all existing data from a stable db state
        _result (q/input query
                  (tx-datoms->zsets2
                    (d/seek-datoms db :eavt)))]
    ;setup listener
    (listen! conn query)))

(defn take-last-datoms
  "Helper to see recently added datoms"
  [conn n]
  (into [] (take n) (d/rseek-datoms @conn :eavt)))

(defn conn-listeners
  "Helper that returns current Datascript conn listeners.
  Returns a map of listener key -> fn (which receives tx-report)
  Warning: This is likely a Datascript implementation detail.
  It could break without warning."
  [conn]
  (:listeners @(:atom conn)))

;examples
(comment
  (set! *print-meta* true)
  (=
    (with-meta
      (datom2 (d/datom 1 :a "v"))
      {:mmmm 42})
    (with-meta
      (datom2 (d/datom 1 :a "v"))
      {:mmmm 43})))
