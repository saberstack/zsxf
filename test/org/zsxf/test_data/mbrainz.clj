(ns org.zsxf.test-data.mbrainz
  (:require [clojure.core.async :as a]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [charred.api :as charred]
            [clj-memory-meter.core :as mm]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.input.datascript :as ds]
            [org.zsxf.datom :as d2]
            [taoensso.nippy :as nippy]
            [org.zsxf.query :as q]
            [org.zsxf.util :as util]
            [org.zsxf.experimental.bifurcan :as clj-bf]
            [org.zsxf.xf :as xf]
            [org.zsxf.zset :as zs]
            [medley.core :as medley]
            [criterium.core :as criterium]
            [datascript.core :as d]
            [datomic.api :as dd]
            [org.zsxf.datomic.cdc :as datomic-cdc]
            [taoensso.timbre :as timbre])
  (:import (clojure.lang IAtom)))

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

(def datomic-schema
  [{:db/valueType   :db.type/string,
    :db/cardinality :db.cardinality/one,
    :db/ident       :artist/name}
   {:db/valueType   :db.type/string,
    :db/cardinality :db.cardinality/one,
    :db/unique      :db.unique/identity,
    :db/ident       :artist/id}
   {:db/valueType   :db.type/ref,
    :db/cardinality :db.cardinality/many,
    :db/ident       :artist/genres}
   {:db/valueType   :db.type/string,
    :db/cardinality :db.cardinality/one,
    :db/unique      :db.unique/identity,
    :db/ident       :genre/name}
   {:db/valueType   :db.type/string,
    :db/cardinality :db.cardinality/one,
    :db/ident       :artist/type}
   {:db/cardinality :db.cardinality/one,
    :db/valueType   :db.type/ref,
    :db/ident       :artist/country}
   {:db/valueType   :db.type/string,
    :db/cardinality :db.cardinality/one,
    :db/unique      :db.unique/identity,
    :db/ident       :country/name-alpha-2}])

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

(defn artist-genres->datomic-refs [genres]
  (into []
    (comp
      (map :name)
      (filter string?)
      (map (fn [genre] {:genre/name genre})))
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

(defn artist->datomic-artist
  [{:keys [name id genres country type]}]
  (cond->
    {:artist/id id}
    (string? name) (assoc :artist/name name)
    (string? country) (assoc :artist/country {:country/name-alpha-2 country})
    (string? type) (assoc :artist/type type)
    (< 0 (count genres)) (assoc :artist/genres (artist-genres->datomic-refs genres))))


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

(defn json-artists->datomic
  [n artists conn & {:keys [artist-xf partition-n]
                     :or   {artist-xf (map identity) partition-n 500}}]
  (let [input-ch  (a/chan 1000)
        output-ch (a/chan 1
                    (comp
                      (partitionv-all partition-n)
                      (remove
                        (fn [artists]
                          (let [tx (time @(dd/transact conn artists))]
                            (timbre/info "artists count:"
                              (count artists))
                            (timbre/info "tx-data count:"
                              (count (:tx-data tx))))
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
        (fn [accum-cnt-final]
          (a/close! input-ch)
          accum-cnt-final))
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


(defn load-artists-from-json->ds [file-path n]
  (with-open [rdr (io/reader file-path)]
    (json-artists->datascript
      n (line-seq rdr)
      :artist-xf
      (comp
        (map (fn [s] (charred/read-json s :key-fn keyword)))
        (map artist->datascript-artist)))))

(defn artists-json->datomic [n file-path conn & {:keys [partition-n]}]
  (with-open [rdr (io/reader file-path)]
    (json-artists->datomic
      n
      (line-seq rdr)
      conn
      :artist-xf (comp
                   (map (fn [s] (charred/read-json s :key-fn keyword)))
                   (map artist->datomic-artist))
      :partition-n partition-n)))

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


(defn count-artists-by-countries-all
  "Query for artist count by all countries."
  [query-state]
  (let [pred-1 #(d2/datom-attr= % :country/name-alpha-2)
        pred-2 #(d2/datom-attr= % :artist/country)]
    (comp
      (xf/mapcat-zset-transaction-xf)
      (map (fn [zset] (xf/disj-irrelevant-items zset pred-1 pred-2)))
      (xf/join-xf
        {:clause    [:c1]
         :pred      pred-1
         :index-kfn d2/datom->eid}
        {:clause    [:c2]
         :pred      pred-2
         :index-kfn d2/datom->val}
        query-state
        :last? true)
      (xforms/reduce zs/zset+)
      ;group by aggregates
      (xf/group-by-xf
        #(-> % (util/nth2 0) d2/datom->val)
        (comp
          (xforms/transjuxt {:cnt (xforms/reduce zs/zset-count+)})
          (mapcat (fn [{:keys [cnt]}]
                    [(zs/zset-count-item cnt)]))))
      (map (fn [final-xf-delta] (timbre/spy final-xf-delta))))))

(defn thaw-artist-datoms!
  "Returns a vector of thawed datoms from the nippy file."
  []
  (nippy/thaw-from-file
    "resources/mbrainz/artists_datoms.nippy"
    {:thaw-xform
     (comp
       (map (fn [thawing]
              (if (vector? thawing)
                (let [[e a v tx b] thawing]
                  (d/datom e a v tx b))
                thawing))))}))

(defn datom->attr-value-map
  [datom]
  (let [[_ a v _ _] datom]
    {a v}))

(defn datoms->attr-value-map
  [datoms]
  (apply merge
    (map datom->attr-value-map datoms)))

(defn datascript-artist-db-conn []
  (d/conn-from-db
    (d/init-db (thaw-artist-datoms!) schema)))

(defn datomic-conn [db-name]
  (dd/connect (datomic-cdc/db-uri db-name)))

(defn conn->db [conn]
  (if (instance? datascript.conn.Conn conn)
    (deref conn)
    (dd/db conn)))

(defn delete-and-init-datomic! []
  (let [^String db-uri (datomic-cdc/db-uri "mbrainz")
        _              (dd/delete-database db-uri)
        _              (dd/create-database db-uri)
        conn           (dd/connect db-uri)
        _              (dd/transact conn datomic-schema)]
    conn))

;Usage
(comment
  (let [conn (delete-and-init-datomic!)]
    (artists-json->datomic
      10000000
      "/Users/raspasov/Downloads/artist/mbdump/artist"
      conn
      :partition-n 10000)))

(defn init-load-all
  "Main loading fn"
  ([^IAtom an-atom]
   (timbre/set-min-level! :info)
   (let [conn (datascript-artist-db-conn)]
     (reset! an-atom conn)
     :done)))

(defn init-query
  "Main fn to setup ZSXF queries with loaded db"
  [query-f conn query-atom]
  (let [query (q/create-query query-f)]
    (reset! query-atom query)
    (ds/init-query-with-conn query conn)
    (q/get-result query)))

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
        ;(count ?a)
        :where
        [?a :artist/country ?c]
        ;[?c :country/name-alpha-2 ?country-name]
        ;[(= ?country-name "US")]
        ]
      @@*conn)))

(defn query-count-artists-by-country-2 [q conn]
  (time
    (q
      '[:find ?country-name (count ?a)
        :where
        [?a :artist/country ?c]
        [?c :country/name-alpha-2 ?country-name]
        [?a :artist/name ?name]]
      (conn->db conn))))

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

  (let [conn (datomic-conn "mbrainz")]
    (query-count-artists-by-country-2 dd/q conn))


  (dd/transact (datomic-conn "mbrainz")
    [{:artist/name    "Eric Jordan"
      :artist/id      "eric-jordan"
      :artist/country [:country/name-alpha-2 "US"]
      :artist/genres  [[:genre/name "electronic"]
                       [:genre/name "trance"]]}])

  )


(comment

  (set! *print-meta* true)
  (timbre/set-min-level! :info)
  (timbre/set-min-level! :trace)


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

  (time
    (init-load-all *conn))

  (ds/unlisten-all! @*conn)


  (time
    (init-query count-artists-by-countries-all @*conn *query-2))

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
