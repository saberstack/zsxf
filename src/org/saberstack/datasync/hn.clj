(ns org.saberstack.datasync.hn
  (:require [charred.api :as charred]
            [clojure.string :as str]
            [medley.core :as medley]
            [org.saberstack.hn.api-v0 :as api]
            [taoensso.nippy :as nippy]
            [babashka.fs :as fs]
            [taoensso.timbre :as timbre]))

(comment
  (nippy/freeze-to-file
    "items-test-2"
    (into []
      (pmap
        api/item!
        (range 1))))

  (nippy/thaw-from-file
    (str "./hndl/" "items-45024001-45025000"))
  )

(defn find-latest-id []
  (sort-by
    (fn [s]
      (parse-long
        (peek (str/split s #"-"))))
    (eduction
      (map str)
      (fs/list-dir "./hndl"))))

(defonce halt? (atom false))
(defonce stopped-at (atom nil))
(defonce sleep-ms (atom 500))
(defonce last-dl (atom nil))

(defn pmap-items! [item-ids]
  (let [resps (pmap api/item! item-ids)]
    {:item-ids    item-ids
     :items       (into [] (map :body) resps)
     :status-200? (every? #(= 200 %) (map :status resps))
     :reached-null? (some? (medley/find-first #(= "null" %) (map :body resps)))
     :resps resps}))

(defn items-to-disk! [items start-id end-id]
  (nippy/freeze-to-file
    (str "./hndl/" "items-" start-id "-" end-id)
    items))

(defn start-sync-items! [start-at-item-id]
  (reset! halt? false)
  (transduce
    (comp
      (partition-all 1000)
      (map pmap-items!)
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
    (iterate inc start-at-item-id)))

(comment

  (reset! halt? true)
  (reset! sleep-ms 20)
  (future
    (start-sync-items! 44642001))
  )
