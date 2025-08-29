(ns org.saberstack.datomic.import
  (:require [babashka.fs :as fs]
            [charred.api :as charred]
            [clj-memory-meter.core :as mm]
            [clojure.core.async :as a]
            [clojure.string :as str]
            [datomic.api :as dd]
            [medley.core :as medley]
            [net.cgrand.xforms :as xforms]
            [org.saberstack.io :as ss.io]
            [org.zsxf.datomic.cdc :as dd.cdc]
            [org.zsxf.input.datomic :as idd]
            [org.zsxf.query :as q]
            [org.zsxf.util :as util]
            [org.zsxf.datalog.compiler :as dcc]
            [taoensso.nippy :as nippy]
            [taoensso.timbre :as timbre]
            [tea-time.core :as tt]))

(declare hn-item->tx-data)
(defonce hn-items (atom []))

;save data to disk at this location
(def ^:static dir-base-path "./hndl/")
(def ^:static path-last-imported (str dir-base-path "_datomic-last-imported"))

;; Load from disk all item files
;; The files are named like "items-500-1000.parquet"
(defn- id-to-sort-by [unix-path]
  (parse-long
    (peek
      (str/split (str (fs/file-name unix-path)) #"\-"))))

(defn get-last-imported-from-disk! []
  (ss.io/try-io!
    (nippy/thaw-from-file
      path-last-imported)))

(defn write-last-imported [file-name]
  (ss.io/try-io!
    (nippy/freeze-to-file
      path-last-imported
      (str file-name))))

(def file-xf
  (comp
    (map (fn [unix-path] (str unix-path)))
    (mapcat (fn [file] (nippy/thaw-from-file file)))
    (map (fn [s] (charred/read-json s :key-fn keyword)))
    (map (fn [m] (hn-item->tx-data m)))))

(defn item-files-to-vector! [files]
  (reset! hn-items [])
  (System/gc)
  (let [{:keys [input-ch output-ch]}
        (util/pipeline-output
          ;write to atom, parquet, etc
          (map (fn [item] (swap! hn-items conj item)))
          ;parallel transform
          file-xf)]
    (a/onto-chan!! input-ch files)
    output-ch))

(defn all-item-files [drop-upto-file-name]
  (into []
    (comp
      (filter (fn [unix-path] (str/starts-with? unix-path "./hndl/items-")))
      (medley/drop-upto #(str/ends-with? % drop-upto-file-name)))
    (sort-by id-to-sort-by
      (fs/list-dir "./hndl"))))

(comment
  (item-files-to-vector!
    (all-item-files
      (get-last-imported-from-disk!)))


  (write-last-imported-file "items-44641001-44642000")
  (count @hn-items))

;; End of section

(def datomic-schema
  [{:db/ident       :hn.user/id
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/doc         "The user's unique username. Case-sensitive. Required."}

   {:db/ident       :hn.user/created
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc         "Creation date of the user, in Unix Time."}

   {:db/ident       :hn.user/karma
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/doc         "The user's karma."}

   {:db/ident       :hn.user/about
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "The user's optional self-description. HTML."}

   {:db/ident       :hn.user/submitted
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc         "List of the user's stories, polls and comments."}

   ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

   {:db/ident       :hn.item/id
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
  (let [^String db-uri (dd.cdc/uri-sqlite "hackernews")
        _              (dd/delete-database db-uri)
        _              (dd/create-database db-uri)
        conn           (dd/connect db-uri)
        _              (dd/transact conn datomic-schema)]
    conn))

(defn hn-conn []
  (let [^String db-uri (dd.cdc/uri-sqlite "hackernews")]
    (dd/connect db-uri)))

(defn- hn-item->tx-data
  "Transforms a HackerNews item map into a Datomic transaction map."
  [m]
  (if (:hn.item/id m)
    m
    (cond-> {:hn.item/id (:id m)}
      (contains? m :deleted) (assoc :hn.item/deleted (boolean (:deleted m)))
      (contains? m :type) (assoc :hn.item/type (keyword (:type m)))
      (contains? m :by) (assoc :hn.item/by (:by m))
      (contains? m :time) (assoc :hn.item/time (java.util.Date. ^long (* 1000 (:time m))))
      (contains? m :text) (assoc :hn.item/text (:text m))
      (contains? m :dead) (assoc :hn.item/dead (:dead m))
      ;;(contains? m :parent) (assoc :hn.item/parent [:hn.item/id (:parent m)])
      ;;(contains? m :poll) (assoc :hn.item/poll [:hn.item/id (:poll m)])
      ;;(contains? m :kids) (assoc :hn.item/kids (mapv (fn [id] [:hn.item/id id]) (:kids m)))
      (contains? m :url) (assoc :hn.item/url (:url m))
      (contains? m :score) (assoc :hn.item/score (:score m))
      (contains? m :title) (assoc :hn.item/title (:title m))
      ;;(contains? m :parts) (assoc :hn.item/parts (mapv (fn [id] [:hn.item/id id]) (:parts m)))
      (contains? m :descendants) (assoc :hn.item/descendants (:descendants m)))))

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
          (timbre/info "transacting..." (count chunk))
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
      (dd/db conn))))

(defn query-unicorn []
  (time
    (let [conn (hn-conn)]
      (dd/q
        '[:find ?e
          :in $
          :where
          [?e :hn.item/title ?text]
          [(clojure.string/lower-case ?text) ?text-lower-case]
          [(clojure.string/includes? ?text-lower-case "unicorn")]]
        (dd/db conn)))))

(defn query-unicorn-fulltext []
  (time
    (let [conn (hn-conn)]
      (dd/q
        '[:find ?e
          :in $
          :where
          [(fulltext $ :hn.item/title "unicorn") [[?e ?a ?v]]]]
        (dd/db conn)))))

(defn get-item-by-id [id]
  (let [conn (hn-conn)]
    (dd/q
      '[:find ?id ?v
        :in $ ?id
        :where
        [?id :hn.item/title ?v]]
      (dd/db conn)
      id)))

(defn get-all-users []
  (let [conn (hn-conn)]
    (time
      (take 100
        (dd/q
          '[:find ?url ?username
            :in $
            :where
            [?e :hn.item/url ?url]
            [?e :hn.item/by ?username]
            ]
          (dd/db conn))))))

(defn get-all-clojure-mentions []
  (dd/q
    '[:find (count ?e)
      :in $
      :where
      [?e :hn.item/text ?txt]
      [(clojure.string/includes? ?txt "clojure")]]
    (dd/db (hn-conn))))

(defonce *query (atom nil))

(defn get-all-clojure-mentions-zsxf []
  (let [conn  (hn-conn)
        query (q/create-query
                (dcc/compile
                  '[:find ?e
                    :where
                    [?e :hn.item/text ?txt]
                    [(clojure.string/includes? ?txt "clojure")]]))
        _     (idd/init-query-with-conn query conn)]
    (reset! *query query)
    :pending))

(defn get-all-users-who-mention-clojure-datomic []
  (dd/q
    '[:find ?username
      :where
      [?e :hn.item/text ?txt]
      [?e :hn.item/by ?username]
      [(clojure.string/includes? ?txt "Clojure")]]
    (dd/db (hn-conn))))


(defn get-all-users-who-mention-clojure-zsxf []
  (let [conn  (hn-conn)
        query (q/create-query
                (dcc/compile
                  '[:find ?username
                    :where
                    [?e :hn.item/by ?username]
                    [?e :hn.item/text ?txt]
                    [(clojure.string/includes? ?txt "Clojure")]]))
        _     (idd/init-query-with-conn query conn)]
    (reset! *query query)
    :pending))


(defn get-all-clojure-mentions-by-raspasov []
  (let [conn  (hn-conn)
        query (q/create-query
                (dcc/compile
                  '[:find ?txt
                    :where
                    [?e :hn.item/by "raspasov"]
                    [?e :hn.item/text ?txt]
                    [(clojure.string/includes? ?txt "Clojure")]]))
        _     (idd/init-query-with-conn query conn)]
    (reset! *query query)
    :pending))

(defn get-all-clojure-posts-by-raspasov []
  (let [conn  (hn-conn)
        query (q/create-query
                (dcc/compile
                  '[:find ?txt
                    :where
                    [?e :hn.item/by "raspasov"]
                    [?e :hn.item/title ?txt]
                    [(clojure.string/includes? ?txt "Clojure")]]))
        _     (idd/init-query-with-conn query conn)]
    (reset! *query query)
    :pending))

(defn get-all-item-ids-via-zsxf []
  (let [conn  (hn-conn)
        query (q/create-query
                (dcc/compile
                  '[:find ?item-id
                    :where
                    [_ :hn.item/id ?item-id]]))
        _     (idd/init-query-with-conn query conn)]
    (reset! *query query)
    :pending))

(defn get-all-users-via-zsxf []
  (let [conn  (hn-conn)
        query (q/create-query
                (dcc/compile
                  '[:find ?username
                    :where
                    [?e :hn.item/url ?url]
                    [?e :hn.item/by ?username]]))
        _     (idd/init-query-with-conn query conn)]
    (reset! *query query)
    :pending))

(defn get-all-users-via-zsxf-single-clause []
  (let [conn  (hn-conn)
        query (q/create-query
                (dcc/compile
                  '[:find ?username
                    :where
                    [?e :hn.item/by ?username]]))
        _     (idd/init-query-with-conn query conn)]
    (reset! *query query)
    :pending))

(defn get-all-users-via-zsxf-from-coll [coll]
  (let [query (q/create-query
                (dcc/compile
                  '[:find ?username
                    :where
                    [?e :hn.item/url ?url]
                    [?e :hn.item/by ?username]]))
        _     (idd/init-query-with-coll query coll)]
    (reset! *query query)
    :pending))

(defonce *query-result-datomic (atom nil))

(defn get-all-users-via-datomic []
  (dd/q
    '[:find ?username
      :where
      [?e :hn.item/by ?username]]
    (dd/db (hn-conn))))

(defn get-all-item-ids-via-datomic []
  (dd/q
    '[:find ?item-id
      :where
      [_ :hn.item/id ?item-id]]
    (dd/db (hn-conn))))

(defn import-progress [conn]
  (let [latest-t (dd/basis-t (dd/db conn))
        {::dd.cdc/keys [last-t-processed start initial-sync-end] :as cdc-stats} (q/cdc-stats @*query)]
    (->
      (q/cdc-stats @*query)
      (merge
        {:progress-percent (double (* 100 (/ last-t-processed latest-t)))
         :basis            [last-t-processed latest-t]})
      (medley/assoc-some
        :sync-total-seconds (when (and start initial-sync-end)
                              (util/nano-to-sec
                                (- initial-sync-end start)))))
    ))

(defonce all-hn-zsets (atom []))
(defonce all-hn-zsets-time (atom nil))

(defn reset-state []
  (ss.loop/stop-all)
  (reset! *query nil)
  (System/gc))

(comment

  (get-all-users-via-zsxf)

  (get-all-users-via-zsxf-single-clause)

  (get-all-item-ids-via-zsxf)

  (get-all-clojure-mentions-zsxf)

  (get-all-clojure-mentions-by-raspasov)

  (time
    (do
      (reset! *query-result-datomic (get-all-item-ids-via-datomic))
      :done))

  (time
    (get-all-users-via-zsxf-from-coll @all-hn-zsets))

  (future
    (util/time-f
      (dd.cdc/log->output
        (atom {})
        (hn-conn)
        idd/zsxf-xform
        (fn
          ([] all-hn-zsets)
          ([accum item]
           (swap! accum conj item)
           accum)
          ([accum] (println :done)))
        nil nil)
      (fn [t] (reset! all-hn-zsets-time t))))

  (reset! *query nil)
  (reset! all-hn-zsets nil)
  (reset! all-hn-zsets-time nil)
  (reset! *query-result-datomic nil)
  (System/gc)

  (import-progress (hn-conn))

  (time
    (do
      (reset! *query-result-datomic (get-all-users-via-datomic))
      :done))

  (mm/measure @*query)
  (count (q/get-result @*query))

  ; Check if the ZSXF query result matches Datomic
  (= (q/get-result @*query) (get-all-users-via-datomic))

  (= (q/get-result @*query) *query-result-datomic)

  (xforms/window window-n rf/avg #(rf/avg %1 %2 -1)))
(set! *warn-on-reflection* true)

(defn files->vector->datomic []
  (let [files     (all-item-files
                    (get-last-imported-from-disk!))
        output-ch (item-files-to-vector! files)]
    (time (a/<!! output-ch))
    (timbre/info "files->vector count :::" (count @hn-items))
    (time (import-items-to-datomic! (hn-conn) @hn-items))
    (when (peek files)
      (write-last-imported (str (peek files))))))

(defn start-datomic-sync-task []
  (tt/every! 10
    (bound-fn []
      (files->vector->datomic))))


(defonce display-task (atom nil))

(defn start-get-result-display []
  (reset! display-task
    (tt/every! 5
      (bound-fn []
        (clojure.pprint/pprint
          (q/get-result @*query))))))

(comment

  (start-get-result-display)

  (start-datomic-sync-task)

  (get-last-imported-from-disk!)
  ;(write-last-imported "items-44642066-44643065")

  ;(delete-and-init-datomic!)

  (hn-conn)

  (count @import-errors)

  (dd/db-stats (dd/db (hn-conn)))

  ;(reset! halt-import? true)
  )
(timbre/set-ns-min-level! :debug)
