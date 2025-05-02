(ns org.zsxf.test-data.movielens-etl
  (:require [tablecloth.api :as tc]
            [datascript.core :as d]
            [clojure.string :as string]
            [libpython-clj2.python :as py]
            [clojure.walk :refer [keywordize-keys]])
  (:import [java.time ZoneId]))

(def key-key (comp keyword
                   string/lower-case))

(comment
  (require '[libpython-clj2.require :refer [require-python]])

  (defn normalize-metadata []
    (-> "resources/movielens/movies_metadata.csv"
        (tc/dataset
         {:key-fn key-key})
        (tc/rename-columns {:id :movie_id})
        (tc/select-columns [:original_title :title :movie_id :overview :imdb_id :poster_path :release_date :tagline])
        (tc/write-csv! "resources/movielens/movies_metadata_normalized.csv")))

  (py/initialize!)
  (require-python 'ast)

  (defn convert-python-col [col]
    (-> col
        .text
        ast/literal_eval
        py/->jvm
        keywordize-keys))

  (defn normalize-credits []
    (-> "resources/movielens/credits.csv"
        (tc/dataset {:key-fn key-key})
        (tc/drop-columns :crew)
        (tc/rename-columns {:id :movie_id})
        (tc/map-rows #(update % :cast convert-python-col))
        (tc/unroll :cast)
        (tc/map-rows (fn [{:keys [cast]}]
                       (select-keys cast [:cast_id :character :id :name :profile_path] )))
        (tc/drop-columns :cast)
        (tc/rename-columns {:id :actor_id})
        (tc/write-csv! "resources/movielens/credits_normalized.csv")))


  (do
    (normalize-metadata)
    (normalize-credits))
  )

(def schema
  {:movie/id {:db/cardinality :db.cardinality/one
              :db/unique      :db.unique/identity}
   :movie/title {:db/cardinality :db.cardinality/one}
   :movie/imdb {:db/cardinality :db.cardinality/one}
   :movie/release-date {:db/cardinality :db.cardinality/one}

   :movie/cast {:db/valueType :db.type/ref
                :db/cardinality :db.cardinality/many}

   :actor/id {:db/cardinality :db.cardinality/one
              :db/unique      :db.unique/identity}
   :actor/name {:db/cardinality :db.cardinality/one}})

(defn local-date->instant [local-date]
  (-> local-date (.atStartOfDay (ZoneId/systemDefault)) (.toInstant)))

(defn populate-datascript-db [conn]
  (do
    ;; First, insert the movies.
    (transduce
     (comp (map (fn [{:keys [movie_id title original_title imdb_id release_date]}]
                  {:movie/id movie_id
                   :movie/title (or title original_title)
                   :movie/imdb imdb_id
                   :movie/release-date (when release_date
                                         (local-date->instant release_date))}))
           (remove (comp nil? :movie/release-date))
           (remove (comp empty? :movie/imdb)))
     (fn ([_ el]
          (d/transact! conn (vector el))
          ())
       ([acc] acc))
     []
     (-> "resources/movielens/movies_metadata_normalized.csv"
         (tc/dataset {:key-fn key-key})
         (tc/rows :as-maps))
     )

    ;; Next, insert the actors.
    (transduce
     (comp (map (fn [{:keys [movie_id actor_id name]}]
                  {:actor/id actor_id
                   :actor/name name})))
     (completing (fn ([_ el]
                      (d/transact! conn (vector el)))))
     []
     (-> "resources/movielens/credits_normalized.csv"
         (tc/dataset {:key-fn key-key})

         (tc/rows :as-maps))
     )

    ;; Finally, insert the movie<-> actor linkages (cast).
    ;; A few cast listings point to movies not in the db, so exclude them.
    (transduce
     (comp (map (fn [{:keys [movie_id actor_id name]}]
                  (let [{movie-eid :db/id}  (d/entity @conn [:movie/id movie_id])
                        {actor-eid :db/id}  (d/entity @conn [:actor/id actor_id])]
                    (when (and movie-eid actor-eid)
                      [:db/add movie-eid :movie/cast actor-eid]))))
           (remove nil?)
           (partition-all 1000))
     (completing (fn ([_ batch]
                      (d/transact! conn batch))))
     []
     (-> "resources/movielens/credits_normalized.csv"
         (tc/dataset {:key-fn key-key})
         (tc/rows :as-maps)))

    ))

(comment
  (def conn (d/create-conn schema))
  (time (populate-datascript-db conn))

  (count (d/datoms @conn :aevt :movie/cast))


  )
