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
            [org.zsxf.datascript :as ds]
            [org.zsxf.query :as q]
            [org.zsxf.util :as util]
            [org.zsxf.xf :as xf]
            [org.zsxf.zset :as zs]
            [datascript.core :as d]
            [taoensso.timbre :as timbre]))

(set! *print-meta* true)

(def schema {:artist/name          {:db/cardinality :db.cardinality/one}
             :artist/id            {:db/cardinality :db.cardinality/one
                                    :db/unique      :db.unique/identity}
             :artist/genres        {:db/cardinality :db.cardinality/many
                                    :db/valueType   :db.type/ref}
             :genre/name           {:db/cardinality :db.cardinality/one
                                    :db/unique      :db.unique/identity}
             :artist/type          {:db/cardinality :db.cardinality/one}
             :artist/country       {:db/cardinality :db.cardinality/one
                                    :db/valueType   :db.type/ref}
             :country/name-alpha-2 {:db/cardinality :db.cardinality/one
                                    :db/unique      :db.unique/identity}})

(defonce *conn (atom (d/create-conn schema)))
(defonce *data (atom []))
(defonce *data-grouped (atom {}))
(defonce *query-1 (atom nil))
(defonce *query-1-times (atom []))
(defonce *query-2 (atom nil))

(defn artist-genres->datascript-refs [genres]
  (into []
    (comp
      (map :name)
      (filter string?)
      (map (fn [genre]
             [:genre/name genre])))
    genres))

(defn artist->datascript-artist
  [{:keys [name id genres country type]}]
  (cond->
    {:artist/id id}
    (string? name) (assoc :artist/name name)
    (string? country) (assoc :artist/country [:country/name-alpha-2 country])
    (string? type) (assoc :artist/type type)
    (< 0 (count genres)) (assoc :artist/genres (artist-genres->datascript-refs genres))))

(def *output
  (atom
    (a/chan 1
      (comp
        (remove
          (fn [artist]
            ;(swap! *data conj output-result)
            (d/transact! @*conn (vector artist))
            true))))))

(defn load-mbrainz [file-path n]
  (with-open [rdr (io/reader file-path)]
    (let [input (a/chan 1000)
          p     (a/pipeline 7
                  @*output
                  (comp
                    (map (fn [s] (charred/read-json s :key-fn keyword)))
                    ;(map (fn [m] (dissoc m :relations :tags :aliases :area :begin-area :end-area :annotation)))
                    (map artist->datascript-artist))
                  input)]
      (doall
        (transduce
          (comp (take n))
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

(defn data->country-set [data]
  (into #{}
    (comp
      (map :country)
      (filter string?)
      (map (fn [country] {:country/name-alpha-2 country})))
    data))

(defn load-country-set []
  (util/load-edn-file "resources/mbrainz/country_set.edn"))

(defn data->genre-set [data]
  (into #{}
    (comp
      (mapcat :genres)
      (map :name)
      (map (fn [genre-name] {:genre/name genre-name})))
    data))

(defn load-genre-set []
  (util/load-edn-file "resources/mbrainz/genre_set.edn"))

(defn pre-load []
  (d/transact! @*conn (vec (load-country-set)))
  (d/transact! @*conn (vec (load-genre-set))))

(defn query-count-artists-by-country-zsxf
  ([query-state]
   (query-count-artists-by-country-zsxf
     query-state
     ;default predicates
     [#(ds/datom-attr-val= % :country/name-alpha-2 "US")
      #(ds/datom-attr= % :artist/country)]))
  ([query-state preds]
   (let [pred-1 (nth preds 0)
         pred-2 (nth preds 1)]
     (comp
       (xf/mapcat-zset-transaction-xf)
       (map (fn [zset] (xf/disj-irrelevant-items zset pred-1 pred-2)))
       (xf/join-xf
         pred-1 ds/datom->eid
         pred-2 ds/datom->val
         query-state
         :last? true)
       (xforms/reduce zs/zset+)
       ;group by aggregates
       (xf/group-by-xf
         #(-> % (util/nth2 0) ds/datom->val)
         (comp
           (xforms/transjuxt {:cnt (xforms/reduce zs/zset-count+)})
           (mapcat (fn [{:keys [cnt]}]
                     [(zs/zset-count-item cnt)]))))
       (map (fn [final-xf-delta] (timbre/spy final-xf-delta)))))))

(defn query-count-artists-by-country-all-zsxf [query-state]
  (let [pred-1 #(ds/datom-attr= % :country/name-alpha-2)
        pred-2 #(ds/datom-attr= % :artist/country)]
    (query-count-artists-by-country-zsxf query-state [pred-1 pred-2])))

(defn init-load-all
  ([] (init-load-all ds/tx-datoms->zsets))
  ([tx-datoms-f]
   (reset! *query-1 (q/create-query query-count-artists-by-country-all-zsxf))
   (reset! *data [])
   (reset! *query-1-times [])
   (reset! *data-grouped {})
   (reset! *conn (d/create-conn schema))
   (d/listen! @*conn :query-1
     (fn [tx-report]
       (util/time-f
         (q/input @*query-1 (tx-datoms-f (:tx-data tx-report)))
         (fn [elapsed-time] (swap! *query-1-times conj elapsed-time)))))
   (pre-load)
   (load-mbrainz "/Users/raspasov/Downloads/artist/mbdump/artist" 10000000)))

(defn init-query [query-atom]
  (reset! query-atom nil)
  (let [query  (q/create-query query-count-artists-by-country-all-zsxf)
        result (time
                 (q/input query
                   (ds/tx-datoms->zsets2
                     (d/seek-datoms @@*conn :eavt))))]
    (reset! query-atom query)
    (timbre/info (mm/measure query))
    result))

(comment
  (init-query *query-1)

  (init-query *query-2)

  (mm/measure [*query-1 *query-2])
  )

(defn query-count-artists-by-genre []
  (time
    (d/q
      '[:find ?genre-name (count ?a)
        :where
        [?a :artist/genres ?g]
        [?g :genre/name ?genre-name]]
      @@*conn)))

(defn query-genres []
  (time
    (d/q
      '[:find ?genre-name
        :where
        [?g :genre/name ?genre-name]]
      @@*conn)))

(defn query-count-artists-by-country []
  (time
    (d/q
      '[:find ?country-name (count ?a)
        :where
        [?c :country/name-alpha-2 ?country-name]
        [?a :artist/country ?c]
        [(= ?country-name "US")]
        ]
      @@*conn)))

(defn query-country []
  (d/q
    '[:find ?c
      :where
      [?c :country/name-alpha-2 "US"]]
    @@*conn))

(defn query-artist []
  (d/q
    '[:find (pull ?a [:artist/name {:artist/country [*]}])
      :where
      [?a :artist/name "deadmau5"]
      [?a :artist/country ?c]]
    @@*conn)

  (d/q
    '[:find (pull ?a [:artist/name {:artist/country [*]}])
      :where
      [?a :artist/name "Eric Jordan"]
      [?a :artist/country ?c]]
    @@*conn))

(comment

  (set! *print-meta* true)
  (timbre/set-min-level! :info)

  (peek @*query-1-times)

  (take 10 (d/seek-datoms @@*conn :eavt))

  (do
    (d/transact! @*conn
      [{:artist/name    "Eric Jordan"
        :artist/id      "eric-jordan"
        :artist/country [:country/name-alpha-2 "US"]
        :artist/genres  [[:genre/name "electronic"]
                         [:genre/name "trance"]]}])
    :done)

  (do
    (d/transact! @*conn
      [{:artist/id      "eric-jordan"
        :artist/country [:country/name-alpha-2 "US"]}])
    :done)

  (do
    (d/transact! @*conn
      [{:artist/name    "Eric Jordan"
        :artist/id      "eric-jordan"
        :artist/country [:country/name-alpha-2 "US"]
        :artist/genres  [[:genre/name "electronic"]
                         [:genre/name "trance"]]}]))

  (do
    (d/transact! @*conn
      [{:artist/name    "Neverrain"
        :artist/id      "Neverrain"
        :artist/country [:country/name-alpha-2 "NZ"]
        :artist/genres  [[:genre/name "electronic"]
                         [:genre/name "trance"]]}])
    :done)

  (do
    (d/transact! @*conn [[:db/retract 140 :country/name-alpha-2 "US"]])
    :done)

  (do
    (d/transact! @*conn [{:country/name-alpha-2 "US"}])
    :done)

  (q/get-state query-1)

  (q/get-result query-1)

  )

(comment
  (do
    (timbre/set-min-level! :info)
    (time (init-load-all ds/tx-datoms->zsets)))

  (do
    (timbre/set-min-level! :info)
    (time (init-load-all ds/tx-datoms->zsets2)))

  (set! *print-meta* true)
  (set! *print-meta* false)

  (q/get-result @*query-1)
  (q/get-state @*query-1)

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

  (mm/measure *conn)
  (mm/measure *query-1)
  (mm/measure [*conn *query-1])


  ;Run 1: (new) via Datom2 (reusing datoms)
  ; (mm/measure *conn)
  ;=> "894.8 MiB"
  ;(mm/measure *query-1)
  ;=> "169.1 MiB"
  ; (mm/measure [*conn *query-1])
  ;=> "1016.5 MiB"
  ;
  ; Result interpretation:
  ; (- 1016 895) ;total memory after, minus DataScript memory
  ;=> 121 ; extra memory (mb) with zsxf query
  ;
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;
  ;Run 2: (old) vector datom (creating new datom vectors)
  ; (mm/measure *conn)
  ;=> "894.8 MiB"
  ;(mm/measure *query-1)
  ;=> "206.3 MiB"
  ;(mm/measure [*conn *query-1])
  ;=> "1.1 GiB"
  ;
  ; Result interpretation:
  ; (- 1100 895)
  ;=> 205 ; extra memory (mb) with zsxf query
  ;
  ;Tldr; re-using datoms via Datom2 seems to be worth it
  ;

  (mm/measure *data)
  (mm/measure [*conn *data])

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
