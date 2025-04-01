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
(defonce *query-1 (atom nil))
(defonce *query-1-times (atom []))
(defonce *query-2 (atom nil))
(defonce *db (atom nil))

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

(defn load-artists [file-path n]
  (with-open [rdr (io/reader file-path)]
    (let [input-ch  (a/chan 1000)
          output-ch (a/chan 1
                      (comp
                        (remove
                          (fn [artist]
                            (d/transact! @*conn (vector artist))
                            ;after transact into Datascript,
                            ; return true to "drop" the data in this transducer
                            ; before it reaches the channel
                            true))))
          _         (a/pipeline 7
                      output-ch
                      (comp
                        (map (fn [s] (charred/read-json s :key-fn keyword)))
                        (map artist->datascript-artist))
                      input-ch)]
      (transduce
        (comp (take n))
        (completing
          (fn [accum-cnt item]
            (a/>!! input-ch item)
            (inc accum-cnt))
          (fn [accum-cnt-final] accum-cnt-final))
        0
        (line-seq rdr)))))

(defn data->country-set [data]
  (into #{}
    (comp
      (map :country)
      (filter string?)
      (map (fn [country] {:country/name-alpha-2 country})))
    data))

(defn data->genre-set [data]
  (into #{}
    (comp
      (mapcat :genres)
      (map :name)
      (map (fn [genre-name] {:genre/name genre-name})))
    data))

(defn load-country-set []
  (util/load-edn-file "resources/mbrainz/country_set.edn"))
(defn load-genre-set []
  (util/load-edn-file "resources/mbrainz/genre_set.edn"))

(defn pre-load []
  (d/transact! @*conn (vec (load-country-set)))
  (d/transact! @*conn (vec (load-genre-set))))

(defn query-count-artists-by-country-zsxf
  "Query for artist count by a specific country."
  [query-state]
  (let [pred-1 #(ds/datom-attr-val= % :country/name-alpha-2 "US")
        pred-2 #(ds/datom-attr= % :artist/country)]
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
      (map (fn [final-xf-delta] (timbre/spy final-xf-delta))))))

(defn query-count-artists-by-all-countries-zsxf
  "Query for artist count by all countries."
  [query-state]
  (let [pred-1 #(ds/datom-attr= % :country/name-alpha-2)
        pred-2 #(ds/datom-attr= % :artist/country)]
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
      (map (fn [final-xf-delta] (timbre/spy final-xf-delta))))))

(defn init-load-all
  ([] (init-load-all ds/tx-datoms->zsets))
  ([_]
   (timbre/set-min-level! :info)
   (reset! *query-1 (q/create-query query-count-artists-by-country-zsxf))
   (reset! *query-2 nil)
   (reset! *query-1-times [])
   (reset! *conn (d/create-conn schema))
   ;setup link between query and connection via Datascript listener
   (ds/init-query-from-empty-db @*conn @*query-1
     :time-f (fn [t] (swap! *query-1-times conj t)))
   ;load countries and genres
   (pre-load)
   ;load artists
   (load-artists "/Users/raspasov/Downloads/artist/mbdump/artist" 10000000)
   ))

(defn recreate-db []
  (let [db (time (d/init-db (d/seek-datoms @@*conn :eavt) schema))]
    (reset! *db db)
    :done))

;Direct Datascript queries
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
      '[:find ?a
        :where
        ;[?c :country/name-alpha-2 ?country-name]
        [?a :artist/country 141]
        ;[(= ?country-name "US")]
        ]
      @@*conn)))

(defn query-all-countries []
  (d/q
    '[:find ?c ?name
      :where
      [?c :country/name-alpha-2 ?name]]
    @@*conn))

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

;end queries
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(comment
  (set! *print-meta* true)
  (timbre/set-min-level! :info)
  (timbre/set-min-level! :trace)

  (take 10 (d/seek-datoms @@*conn :eavt))

  (do
    ;add another artist
    (d/transact! @*conn
      [{:artist/name    "Eric Jordan"
        :artist/id      "eric-jordan"
        :artist/country [:country/name-alpha-2 "US"]
        :artist/genres  [[:genre/name "electronic"]
                         [:genre/name "trance"]]}])
    :done)

  (do
    ;retract artist
    (d/transact! @*conn [[:db/retractEntity [:artist/id "eric-jordan"]]])
    :done)


  (do
    ;retract country
    (d/transact! @*conn [[:db/retractEntity [:country/name-alpha-2 "US"]]])
    :done)

  )
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(comment
  ;second query
  (do
    (timbre/set-min-level! :info)
    (let [query (q/create-query query-count-artists-by-all-countries-zsxf)]
      (ds/init-query-from-existing-db @*conn query)
      (reset! *query-2 query)
      (q/get-result query)))

  (ds/take-last-datoms @*conn 20)

  (mm/measure [*query-1 *query-2])

  (mm/measure *db)
  (mm/measure *conn)
  (mm/measure [*db *conn])

  )

(comment

  (do
    (time (init-load-all ds/tx-datoms->zsets2)))

  (set! *print-meta* true)
  (set! *print-meta* false)

  (q/get-result @*query-1)
  (q/get-state @*query-1)


  (mm/measure "Hello, memory meter!")
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


  )
