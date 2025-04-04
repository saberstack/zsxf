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
            [taoensso.nippy :as nippy]
            [org.zsxf.query :as q]
            [org.zsxf.util :as util]
            [org.zsxf.experimental.bifurcan :as clj-bf]
            [org.zsxf.xf :as xf]
            [org.zsxf.zset :as zs]
            [criterium.core :as criterium]
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
(defonce *query-2 (atom nil))
(defonce *query-3 (atom nil))
(defonce *conn-2 (atom nil))

(defn artist-genres->datascript-refs [genres]
  (into []
    (comp
      (map :name)
      (filter string?)
      (map (fn [genre]
             [:genre/name genre])))
    genres))

(defonce artists-full-set (atom #{}))

(defn artist->datascript-artist
  [{:keys [name id genres country type]}]
  (cond->
    {:artist/id id}
    (string? name) (assoc :artist/name name)
    (string? country) (assoc :artist/country [:country/name-alpha-2 country])
    (string? type) (assoc :artist/type type)
    (< 0 (count genres)) (assoc :artist/genres (artist-genres->datascript-refs genres))))


(defn json-artists->datascript
  [n artists & {:keys [artist-xf] :or {artist-xf (map identity)}}]
  (let [input-ch  (a/chan 1000)
        output-ch (a/chan 1
                    (comp
                      (remove
                        (fn [artist]
                          (d/transact! @*conn (vector artist))
                          ; return true to "drop" the data in this transducer
                          ; before it reaches the channel
                          true))))
        _         (a/pipeline 7
                    output-ch
                    artist-xf
                    input-ch)]
    (transduce
      (comp (take n))
      (completing
        (fn [accum-cnt item]
          (a/>!! input-ch item)
          (inc accum-cnt))
        (fn [accum-cnt-final] accum-cnt-final))
      0
      artists)))

(defn nippy-artists->datascript
  [n artists]
  (transduce
    (comp (take n))
    (completing
      (fn [accum-cnt artist]
        (d/transact! @*conn (vector artist))
        ; return true to "drop" the data in this transducer
        ; before it reaches the channel
        true
        (inc accum-cnt))
      (fn [accum-cnt-final] accum-cnt-final))
    0
    artists))

(defn load-artists-from-nippy [file-path n]
  (nippy-artists->datascript n (nippy/thaw-from-file file-path)))

(defn load-artists-from-json [file-path n]
  (with-open [rdr (io/reader file-path)]
    (json-artists->datascript
      n (line-seq rdr)
      :artist-xf
      (comp
        (map (fn [s] (charred/read-json s :key-fn keyword)))
        (map artist->datascript-artist)))))

(comment
  (nippy/freeze-to-file "resources/mbrainz/artists_mini_set.nippy"
    (into #{}
      (util/keep-every-nth 100)
      @artists-full-set)))

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
  (util/read-edn-file "resources/mbrainz/country_set.edn"))
(defn load-genre-set []
  (util/read-edn-file "resources/mbrainz/genre_set.edn"))


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

(defn init-load-all []
  (timbre/set-min-level! :info)

  (reset! *query-1 (q/create-query query-count-artists-by-country-zsxf))
  (reset! *query-2 nil)
  (reset! *query-3 nil)

  (reset! *conn (d/create-conn schema))
  ;setup link between query and connection via Datascript listener
  (ds/init-query-with-conn @*query-1 @*conn)
  ;load countries and genres
  (d/transact! @*conn (vec (load-country-set)))
  (d/transact! @*conn (vec (load-genre-set)))

  ;load artists
  ;(load-artists-from-json "/Users/raspasov/Downloads/artist/mbdump/artist" 10000000)
  ;(load-artists-from-nippy "resources/mbrainz/artists_mini_set.nippy" 10000000)
  (load-artists-from-nippy "resources/mbrainz/artists_set.nippy" 10000000))



(defn init-db-fast
  ([] (init-db-fast (d/seek-datoms @@*conn :eavt)))
  ([datoms]
   (let [db (d/init-db datoms schema)]
     (reset! *conn-2 (d/conn-from-db db))
     :done)))

(comment
  (reset! *conn-2 nil)

  (let [stage-1 (time (nippy/freeze-to-file
                        "resources/mbrainz/artists_datoms.nippy"
                        (into
                          []
                          (comp
                            (map (fn [[e a v tx b]]
                                   [e a v tx b])))
                          (d/seek-datoms @@*conn :eavt))))])

  (let [stage-2 (time
                  (nippy/thaw-from-file
                    "resources/mbrainz/artists_datoms.nippy"
                    {:thaw-xform
                     (comp
                       (map (fn [thawing]
                              (if (vector? thawing)
                                (let [[e a v tx b] thawing]
                                  (d/datom e a v tx b))
                                thawing))))}))
        state-3 (time (init-db-fast stage-2))]
    :done)

  )

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
      '[:find ?country-name (count ?a)
        :where
        [?a :artist/country ?c]
        [?c :country/name-alpha-2 ?country-name]
        [(= ?country-name "US")]
        ]
      @@*conn)))

(defn query-count-artists-by-country-2 [conn]
  (time
    (d/q
      '[:find ?country-name (count ?a)
        :where
        [?a :artist/country ?c]
        [?c :country/name-alpha-2 ?country-name]
        [(= ?country-name "US")]
        ]
      @conn)))

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


(defn init-query [conn query-atom]
  (do
    (timbre/set-min-level! :info)
    (when-let [query @query-atom]
      (timbre/info "unlisten...")
      (d/unlisten! conn (q/get-id query))
      :ok)
    (let [query (q/create-query query-count-artists-by-all-countries-zsxf)]
      (reset! query-atom query)
      (ds/init-query-with-conn query conn)
      (q/get-result query))))

(comment

  (init-query @*conn-2 *query-2)

  (set! *print-meta* true)
  (timbre/set-min-level! :info)
  (timbre/set-min-level! :trace)

  (take 10 (d/rseek-datoms @@*conn :eavt))

  (do
    ;add another artist
    (d/transact! @*conn-2
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
  (time
    (do
      (timbre/set-min-level! :info)
      (let [query (q/create-query query-count-artists-by-all-countries-zsxf)]
        (reset! *query-2 query)
        (ds/init-query-with-conn query @*conn)
        (q/get-result query))))

  (do
    (timbre/set-min-level! :info)
    (let [query (q/create-query query-count-artists-by-country-zsxf)]
      (reset! *query-3 query)
      (ds/init-query-with-conn query @*conn)
      (q/get-result query)))

  ;unlisten
  (do
    (d/unlisten! @*conn (q/get-id @*query-2))
    :done)

  ;re-listen
  (do
    (ds/listen! @*conn @*query-1)
    :done)

  (do
    (d/unlisten! @*conn (q/get-id @*query-1))
    :done)

  (do
    (d/unlisten! @*conn (q/get-id @*query-2))
    :done)



  (do
    (d/unlisten! @*conn (q/get-id @*query-3))
    :done)

  (ds/take-last-datoms @*conn 20)



  (mm/measure *conn-2)

  (mm/measure [*conn])

  (mm/measure [*conn *query-1])

  (q/get-id @*query-1)


  (mm/measure [*conn-2 *conn])

  )

(comment

  (do
    (time (init-load-all)))

  (set! *print-meta* true)
  (set! *print-meta* false)

  (q/get-result @*query-1)
  (q/get-result @*query-2)
  (q/get-state @*query-1)


  (mm/measure "Hello, memory meter!")
  (mm/measure *conn)
  (mm/measure *query-1)
  (mm/measure [*conn *query-1])

  (mm/measure [*conn *query-1])

  (mm/measure [*query-1])
  (mm/measure [*query-2])
  (mm/measure [*conn *query-1 *query-2])


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

(defonce *bifurcan-map (atom nil))
(defonce *clojure-map (atom nil))
(defonce *clojure-vector (atom nil))

(comment
  ;bifurcan map
  (time
    (do
      (reset!
        *bifurcan-map
        (transduce
          (map (fn [datom]
                 [datom datom]))
          conj
          (clj-bf/clj-bf-map)
          (take 10000000
            (d/seek-datoms @@*conn :eavt))))
      :done))

  ;clojure map
  (time
    (do
      (reset!
        *clojure-map
        (transduce
          (map (fn [datom]
                 [datom datom]))
          conj
          {}
          (take 10000000
            (d/seek-datoms @@*conn :eavt))))
      :done))

  ;clojure vector
  (time
    (do
      (reset!
        *clojure-vector
        (transduce
          (map (fn [datom]
                 datom))
          conj
          []
          (take 10000000
            (d/seek-datoms @@*conn :eavt))))
      :done))

  ;Clojure vector (separate from maps)
  (mm/measure *clojure-vector)

  (time
    (transduce
      (map (fn [e]))
      (completing
        (fn [accum item]
          accum))
      :done
      @*clojure-vector))
  ;vectors are fast to iterate
  ; "Elapsed time: 144.049291 msecs"

  ;Maps...

  ;bf memory usage seems a bit higher...
  (mm/measure *bifurcan-map)
  ;=> "1.1 GiB
  ;vs clojure map
  (mm/measure *clojure-map)
  ;=> "1013.4 MiB"
  ;

  ;... but faster iteration!
  (time
    (transduce
      (map (fn [e]))
      (completing
        (fn [accum item]
          accum))
      :done
      @*bifurcan-map))
  ;Approx "Elapsed time: 215 msecs"

  (time
    (transduce
      (map (fn [e]))
      (completing
        (fn [accum item]
          accum))
      :done
      @*clojure-map))
  ;Approx "Elapsed time: 500 msecs"

  )
