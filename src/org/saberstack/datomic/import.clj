(ns org.saberstack.datomic.import
  (:require [babashka.fs :as fs]
            [charred.api :as charred]
            [clojure.core.async :as a]
            [clojure.string :as str]
            [datomic.api :as dd]
            [org.zsxf.datomic.cdc :as datomic-cdc]
            [org.zsxf.util :as util]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as timbre]))

(defonce hn-items (atom []))

;; Load from disk all item files
;; The files are named like "items-500-1000.parquet"
(defn- id-to-sort-by [unix-path]
  (parse-long
    (peek
      (str/split (str (fs/file-name unix-path)) #"\-"))))

(defn all-item-files []
  (sort-by id-to-sort-by
    (eduction
      (filter (fn [unix-path] (str/starts-with? unix-path "./hndl/items-")))
      (fs/list-dir "./hndl"))))

(def file-xf
  (comp
    (map (fn [unix-path] (str unix-path)))
    (mapcat (fn [file] (nippy/thaw-from-file file)))
    (map (fn [s] (charred/read-json s :key-fn keyword)))))

(defn thaw-item-files [files]
  (reset! hn-items [])
  (System/gc)
  (let [input-ch (util/pipeline-output
                   ;write to atom, parquet, etc
                   (map (fn [item] (swap! hn-items conj item)))
                   ;parallel transform
                   file-xf)]
    (a/onto-chan!! input-ch files)))

(comment
  (thaw-item-files (all-item-files))
  (count @hn-items)
  )

;; End of section

(def datomic-schema
  [{:db/ident       :hn.item/id
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "The item's unique id from HackerNews."}

   {:db/ident       :hn.item/deleted
    :db/valueType   :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/doc         "true if the item is deleted."}

   {:db/ident       :hn.item/type
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index       true
    :db/doc         "The type of item. One of :job, :story, :comment, :poll, or :pollopt."}

   {:db/ident       :hn.item/by
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index       true
    :db/doc         "The username of the item's author."}

   {:db/ident       :hn.item/time
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc         "Creation date of the item."}

   {:db/ident       :hn.item/text
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "The comment, story or poll text. Can contain HTML."}

   {:db/ident       :hn.item/dead
    :db/valueType   :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/doc         "true if the item is dead."}

   {:db/ident       :hn.item/parent
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "The comment's parent: either another comment or the relevant story."}

   {:db/ident       :hn.item/poll
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "The pollopt's associated poll."}

   {:db/ident       :hn.item/kids
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc         "The ids of the item's comments, in ranked display order."}

   {:db/ident       :hn.item/url
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "The URL of the story."}

   {:db/ident       :hn.item/score
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc         "The story's score, or the votes for a pollopt."}

   {:db/ident       :hn.item/title
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/fulltext    true
    :db/doc         "The title of the story, poll or job. Can contain HTML."}

   {:db/ident       :hn.item/parts
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc         "A list of related pollopts, in display order."}

   {:db/ident       :hn.item/descendants
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc         "In the case of stories or polls, the total comment count."}])

(defn delete-and-init-datomic! []
  (let [^String db-uri (datomic-cdc/uri-sqlite "hackernews")
        _              (dd/delete-database db-uri)
        _              (dd/create-database db-uri)
        conn           (dd/connect db-uri)
        _              (dd/transact conn datomic-schema)]
    conn))

(defn hn-conn []
  (let [^String db-uri (datomic-cdc/uri-sqlite "hackernews")]
    (dd/connect db-uri)))

(defn- hn-item->tx-data
  "Transforms a HackerNews item map into a Datomic transaction map."
  [item]
  (if (:hn.item/id item)
    item
    (cond-> {:hn.item/id (:id item)}
      (contains? item :deleted) (assoc :hn.item/deleted (boolean (:deleted item)))
      (contains? item :type) (assoc :hn.item/type (keyword (:type item)))
      (contains? item :by) (assoc :hn.item/by (:by item))
      (contains? item :time) (assoc :hn.item/time (java.util.Date. (* 1000 (:time item))))
      (contains? item :text) (assoc :hn.item/text (:text item))
      (contains? item :dead) (assoc :hn.item/dead (:dead item))
      ;(contains? item :parent) (assoc :hn.item/parent [:hn.item/id (:parent item)])
      ;(contains? item :poll)    (assoc :hn.item/poll [:hn.item/id (:poll item)])
      ;(contains? item :kids)    (assoc :hn.item/kids (mapv (fn [id] [:hn.item/id id]) (:kids item)))
      (contains? item :url) (assoc :hn.item/url (:url item))
      (contains? item :score) (assoc :hn.item/score (:score item))
      (contains? item :title) (assoc :hn.item/title (:title item))
      ;(contains? item :parts)   (assoc :hn.item/parts (mapv (fn [id] [:hn.item/id id]) (:parts item)))
      (contains? item :descendants) (assoc :hn.item/descendants (:descendants item)))))

(defn hn-item-tx-data-xf [num-of-chunks chunk-size]
  (comp
    (map hn-item->tx-data)
    (partition-all chunk-size)
    (take (or num-of-chunks Long/MAX_VALUE))))

(comment
  (set! *print-namespace-maps* false)
  (into []
    (hn-item-tx-data-xf 1 100)
    @hn-items)
  )

(defonce import-errors (atom []))
(defonce halt-import? (atom false))
(defn halt-now? [_] @halt-import?)

(defn import-items-to-datomic! [conn items]
  (reset! halt-import? false)
  (reset! import-errors [])
  ;; Transact in chunks to avoid overwhelming the transactor.
  (transduce
    (comp
      (hn-item-tx-data-xf nil 100)
      (halt-when halt-now?))
    (completing
      (fn [conn chunk]
        (try
          @(dd/transact conn chunk)
          (catch Exception e
            (timbre/error "Error during transaction:" (.getMessage e))
            (swap! import-errors conj {:chunk chunk :error (.getMessage e)})))
        conn))
    conn
    items))

(defonce re-import (atom []))

(defn item-patch-deleted [m]
  (update m :hn.item/deleted boolean))

(defn errors->items [errors]
  (into []
    (comp
      (mapcat :chunk)
      (map item-patch-deleted))
    errors))

(comment
  (reset! re-import (errors->items @import-errors))
  (count @re-import)
  (import-items-to-datomic! (hn-conn) @re-import)
  )

(defn count-number-of-comments []
  (let [conn (hn-conn)]
    (dd/q
      '[:find (count ?e)
        :in $
        :where [?e :hn.item/id _]]
      (dd/db conn))
    )
  )

(comment

  (delete-and-init-datomic!)

  (hn-conn)

  (future
    (time
      (import-items-to-datomic! (hn-conn) @hn-items)))

  (count @import-errors)

  (dd/db-stats (dd/db (hn-conn)))

  ;(reset! halt-import? true)
  )
