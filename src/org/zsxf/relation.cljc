(ns org.zsxf.relation
  (:require [org.zsxf.xf :as-alias xf]))

(defn relation? [x]
  (boolean
    (and
      (indexed? x)
      (= (count x) 2)
      (:xf.clause (meta (first x)))
      (:xf.clause (meta (second x))))))

(defn index-clauses [rel]
  (if (relation? rel)
    (let [existing-rel-index-1 (:rel.index (meta (first rel)))
          existing-rel-index-2 (:rel.index (meta (second rel)))
          rel-index-from-existing
                               (merge
                                 (update-vals existing-rel-index-1 (fn [v] (apply conj [0] v)))
                                 (update-vals existing-rel-index-2 (fn [v] (apply conj [1] v))))
          rel-index-m          (merge-with
                                 (fn [val-in-prev _val-in-latter]
                                   val-in-prev)
                                 rel-index-from-existing
                                 (into {}
                                   (map-indexed
                                     (fn [idx rel-item]
                                       [(:xf.clause (meta rel-item)) [idx]]))
                                   rel))]
      (-> rel
        (vary-meta (fn [m] (assoc m :rel.index rel-index-m)))
        (update 0 (fn [x] (vary-meta x #(dissoc % :rel.index))))
        (update 1 (fn [x] (vary-meta x #(dissoc % :rel.index))))))
    rel))

(defn find-clause [rel clause]
  (let [path (get (:rel.index (meta rel)) clause)]
    (when path
      (get-in rel path))))

(comment
  (let [rel
        ^{:zset/w    1,
          :rel.index {'[?p :person/name "Alice"]        [0 0],
                      '[?p :person/country ?c]          [0],
                      '[?c :country/continent "Europe"] [1]}}
        [^{:xf.clause '[?p :person/country ?c]}
         [^{:xf.clause '[?p :person/name "Alice"]} [2 :person/name "Alice"]
          ^{:xf.clause '[?p :person/country ?c]} [2 :person/country 1]]
         ^{:xf.clause '[?c :country/continent "Europe"]}
         [1 :country/continent "Europe"]]]

    (find-clause rel '[?p :person/country ?c])))
