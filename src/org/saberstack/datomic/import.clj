(ns org.saberstack.datomic.import
  (:require [babashka.fs :as fs]
            [charred.api :as charred]
            [clojure.core.async :as a]
            [clojure.string :as str]
            [org.zsxf.util :as util]
            [taoensso.nippy :as nippy]))

(defonce tmp-hn-items (atom []))

(defn- id-to-sort-by [unix-path]
  (parse-long
    (peek
      (str/split (str (fs/file-name unix-path)) #"\-"))))

(defn all-item-files []
  (sort-by id-to-sort-by
    (eduction
      (filter (fn [unix-path] (str/starts-with? unix-path "./hndl/items-")))
      (fs/list-dir "./hndl"))))

(defn thaw-item-files [files]
  (reset! tmp-hn-items [])
  (System/gc)
  (let [input-ch (util/pipeline-output
                   ;write to atom, parquet, etc
                   (map (fn [item] (swap! tmp-hn-items conj item)))
                   ;parallel transform
                   (comp
                     (map (fn [unix-path] (str unix-path)))
                     (mapcat (fn [file] (nippy/thaw-from-file file)))
                     (map (fn [s] (charred/read-json s :key-fn keyword)))))]
    (a/onto-chan!! input-ch files)))

(comment
  (thaw-item-files (all-item-files))
  (count @tmp-hn-items))
