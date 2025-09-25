(ns org.saberstack.datasync.hn
  (:require [charred.api :as charred]
            [clojure.string :as str]
            [medley.core :as medley]
            [org.saberstack.hn.api-v0 :as api]
            [org.saberstack.io :as ss.io]
            [taoensso.nippy :as nippy]
            [babashka.fs :as fs]
            [tea-time.core :as tt]
            [taoensso.timbre :as timbre]))

;save data to disk at this location
(def ^:static dir-base-path "./hndl/")
(def ^:static path-last-item-id (str dir-base-path "_max-item-id"))

(defonce halt? (atom false))
(defonce stopped-at (atom nil))
(defonce sleep-ms (atom 500))
(defonce last-dl (atom nil))

(defn- body->id [^String body]
  (ss.io/try-io!
    (-> body (charred/read-json :key-fn keyword) :id long)))

(defn get-parsed-items! [item-ids start]
  (let [null-pred     #(= "null" %)
        resps         (ss.io/try-io! (into [] (pmap api/item! item-ids)))
        items         (into [] (comp (map :body) (medley/take-upto null-pred)) resps)
        reached-null? (some? (medley/find-first null-pred items))
        items'        (if reached-null?
                        (vec (butlast (medley/take-upto null-pred items)))
                        items)
        [first-synced last-synced]
        (if-let [body (first items')]
          [(body->id body) (body->id (peek items'))]
          ;rare case where the first item's body is "null";
          ;tldr; HN API is special
          [start start])]
    {:status-200?   (every? #(= 200 %) (map :status resps))
     :item-ids      item-ids
     :items         items
     :reached-null? reached-null?
     :resps         resps
     :first-synced  first-synced
     :last-synced   last-synced}))

(defn items-to-disk! [items start-id end-id]
  (ss.io/try-io!
    (nippy/freeze-to-file
      (str "./hndl/" "items-" start-id "-" end-id)
      items)))

(defn write-last-item-id-to-disk! [item-id]
  (ss.io/try-io!
    (nippy/freeze-to-file
      path-last-item-id
      item-id)))

(defn get-last-item-id-from-disk! []
  (ss.io/try-io!
    (nippy/thaw-from-file
      path-last-item-id)))

(defn get-hn-max-item-id!
  "Get and parse HN max item."
  []
  (ss.io/try-io!
    (-> (api/max-item!) :body parse-long)))

(defn sync-items! [prev-end end]
  (reset! halt? false)
  (let [start (inc prev-end)]
    (transduce
      (comp
        (partition-all 1000)
        (map (fn [item-ids] (get-parsed-items! item-ids start)))
        (map (fn [{:keys [items item-ids status-200? resps first-synced last-synced] :as m}]
               (if status-200?
                 (do
                   (when (and (int? first-synced) (int? last-synced))
                     (items-to-disk! items first-synced last-synced)
                     (write-last-item-id-to-disk! last-synced)
                     (timbre/info [::sync-ok [first-synced last-synced]]))
                   (reset! last-dl [(first item-ids) (peek item-ids)]))
                 ;else
                 (reset! stopped-at resps))
               ;return
               m))
        (halt-when (fn [{:keys [status-200? reached-null? last-synced]}]
                     (or
                       (not status-200?)
                       (nil? last-synced)
                       (true? reached-null?)
                       (true? @halt?))))
        (map (fn [_] :ok)))
      conj
      (range start (inc end)))))

(timbre/set-ns-min-level! :debug)

(defonce hn-sync-task (atom nil))

(defn start-hn-sync-task []
  (reset! hn-sync-task
    (tt/every! 5
      (bound-fn []
        (let [item-id-last-synced (get-last-item-id-from-disk!)
              item-id-max         (get-hn-max-item-id!)]
          (if (and
                (int? item-id-max)
                (int? item-id-last-synced)
                (< item-id-last-synced item-id-max))
            (do
              (timbre/info [::next [item-id-last-synced item-id-max]])
              (timbre/info (sync-items! item-id-last-synced item-id-max)))
            (timbre/info [::wait [item-id-last-synced item-id-max]])))))))

;REPL task control
(comment
  (tt/reset-tasks!)
  (tt/start!)
  (start-hn-sync-task)
  (:body (api/max-item!))


  (get-last-item-id-from-disk!)

  )

(comment
  (nippy/freeze-to-file
    "items-test-2"
    (into []
      (pmap
        api/item!
        (range 1))))

  (nippy/thaw-from-file
    (str dir-base-path "items-44642001-44643000")))
