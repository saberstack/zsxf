(ns org.zsxf.test-data.mbrainz
  (:require [clojure.core.async :as a]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.data.json]
            [charred.api :as charred]
            [medley.core :as medley]
            [clj-memory-meter.core :as mm]
            [ham-fisted.api :as hf]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.zset :as zs]
            [datascript.core :as d]
            [taoensso.timbre :as timbre]))

(defonce cnt (atom 0))

(defonce *data (atom (hf/vector)))

(defonce *data-grouped (atom (hf/immut-map)))

(def *output
  (atom
    (a/chan (a/sliding-buffer 1)
      (map
        (fn [output-result]
          (swap! *data conj output-result))))))

(def schema {:artist/name          {:db/cardinality :db.cardinality/one}
             :artist/id            {:db/cardinality :db.cardinality/one
                                    :db/unique      :db.unique/identity}
             :artist/genre         {:db/cardinality :db.cardinality/many
                                    :db/valueType   :db.type/ref}
             :genre/name           {:db/cardinality :db.cardinality/one
                                    :db/unique      :db.unique/identity}
             :artist/type          {:db/cardinality :db.cardinality/one}
             :artist/country       {:db/cardinality :db.cardinality/one
                                    :db/valueType   :db.type/ref}
             :country/name-alpha-2 {:db/cardinality :db.cardinality/one
                                    :db/unique      :db.unique/identity}})

(defn load-mbrainz [file-path]
  (reset! cnt 0)
  (reset! *data (hf/vector))
  (reset! *data-grouped (hf/immut-map))
  (with-open [rdr (io/reader file-path)]
    (let [input (a/chan 1000)
          p     (a/pipeline 7
                  @*output
                  (comp
                    (map (fn [s] (charred/read-json s :key-fn keyword)))
                    (map (fn [m] (dissoc m :relations :tags :aliases :area :begin-area :end-area :annotation))))
                  input)]
      (doall
        (transduce
          (comp (take 10000000 #_10000000))
          (completing
            (fn [accum-cnt item]
              (a/>!! input item)
              (inc accum-cnt))
            (fn [accum-cnt-final]
              (timbre/spy accum-cnt-final)
              ;(a/close! input)
              [accum-cnt-final]
              ))
          0
          (line-seq rdr))))))


(comment
  (time (load-mbrainz "/Users/raspasov/Downloads/artist/mbdump/artist"))
  (a/<!! @*output)

  (medley/find-first
    (fn [m] (= "Linkin Park" (get m :name)))
    @*data)

  (medley/find-first
    (fn [m] (str/starts-with? (get m :name) "deadmau"))
    @*data)

  (medley/find-first
    (fn [m] (nil? (get m :id)))
    @*data)

  (medley/find-first
    (fn [m] (= nil (:name m)))
    @*data)

  (count
    (into #{}
      (comp
        (mapcat :genres)
        (map (juxt :id :name))
        )
      @*data))

  (count
    (into #{}
      (comp
        (map :gender)
        )
      @*data))

  (mm/measure "Hello, meter!")

  (mm/measure @*data)

  (mm/measure [@*data @*data-grouped])

  (time
    (do
      (reset! *data-grouped
        (into
          {}
          (comp
            ;(map (fn [m] (zs/zset-item m 1)))
            (zs/index-xf :name #{}))
          @*data))
      :done))

  (time
    (do
      (reset! *data-grouped
        (into
          (hf/hash-map)
          (comp
            ;(map (fn [m] (zs/zset-item m 1)))
            (zs/index-xf :name (hf/immut-set)))
          @*data))
      :done))

  (do
    (reset! *data-grouped
      (into
        {}
        (comp
          (map (fn [[k ps]]
                 [k (into []
                      (map (fn [m] (vary-meta m (fn [_]))))
                      ps)])))
        @*data-grouped))
    :done)

  (identical?
    (first
      (vary-meta
        (first (get @*data-grouped "Linkin Park"))
        (fn [_])))

    (first
      (medley/find-first
        (fn [m] (= "Linkin Park" (:name m)))
        @*data)))


  (transduce
    (filter (fn [[_ v]] (< 1 (count v))))
    (xforms/count +)
    @*data-grouped)
  ;=> 107924

  )
