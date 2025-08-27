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

(defn get-parsed-items! [item-ids]
  (let [resps (ss.io/try-io! (doall (pmap api/item! item-ids)))]
    {:item-ids      item-ids
     :items         (into [] (map :body) resps)
     :status-200?   (every? #(= 200 %) (map :status resps))
     :reached-null? (some? (medley/find-first #(= "null" %) (map :body resps)))
     :resps         resps}))

(defn items-to-disk! [items start-id end-id]
  (ss.io/try-io!
    (nippy/freeze-to-file
      (str "./hndl/" "items-" start-id "-" end-id)
      items)))

(defn last-item-id-to-disk! [max-item-id]
  (ss.io/try-io!
    (nippy/freeze-to-file
      path-last-item-id
      max-item-id)))

(defn get-last-item-id-from-disk! []
  (nippy/thaw-from-file
    path-last-item-id))

(defn get-hn-max-item!
  "Get and parse HN max item."
  []
  (let [x "test"]
    (ss.io/try-io!
      (-> (api/max-item!) :body parse-long))))

(defn start-sync-items! [item-id-start item-id-max]
  (reset! halt? false)
  (transduce
    (comp
      (partition-all 1000)
      (map get-parsed-items!)
      (map (fn [{:keys [items item-ids status-200? reached-null? resps] :as m}]
             (if status-200?
               (do
                 (items-to-disk! items (first item-ids) (peek item-ids))
                 (reset! last-dl [(first item-ids) (peek item-ids)])
                 (assoc m :download-ok? true))
               (do
                 (reset! stopped-at resps)
                 (assoc m :download-ok? false)))))
      (halt-when (fn [{:keys [download-ok? reached-null?]}]
                   (or
                     (not download-ok?)
                     (true? reached-null?)
                     (true? @halt?))))
      (map (fn [_] (Thread/sleep ^long @sleep-ms))))
    conj
    (iterate inc item-id-start)))

(timbre/set-ns-min-level! :debug)

(defn start-refresh-task []
  (tt/every! 5
    (bound-fn []
      (timbre/info "I don't refresh anything, yet")
      (let [max-item-id (get-hn-max-item!)]
        (when (int? max-item-id)
          (last-item-id-to-disk! max-item-id))))))

;REPL task control
(comment
  (tt/reset-tasks!)
  (start-refresh-task)
  (:body (api/max-item!))


  (get-last-item-id-from-disk!)

  )

(comment
  (reset! halt? true)
  (reset! sleep-ms 20)
  (future
    (start-sync-items! 44642001)))

(comment
  (nippy/freeze-to-file
    "items-test-2"
    (into []
      (pmap
        api/item!
        (range 1))))

  (nippy/thaw-from-file
    (str dir-base-path "items-45024001-45025000")))
