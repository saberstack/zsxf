(ns org.zsxf.relation
  (:require [org.zsxf.xf :as-alias xf]))

(defn relation? [x]
  (boolean
    (and
      (indexed? x)
      (= (count x) 2)
      (:xf.clause (meta (first x)))
      (:xf.clause (meta (second x))))))

(defn index-clauses [rel1+rel2-v]
  rel1+rel2-v
  #_(if (relation? rel1+rel2-v)
    (let [[rel1 rel2] rel1+rel2-v
          rel-index-1-prev (:rel.index (meta rel1))
          rel-index-2-prev (:rel.index (meta rel2))
          ;"Lift" previous relation indices one level
          rel-index-prev'  (merge
                             (update-vals rel-index-1-prev (fn [v] (apply conj [0] v)))
                             (update-vals rel-index-2-prev (fn [v] (apply conj [1] v))))
          ;next index from top level clauses
          rel-index-next   {(:xf.clause (meta rel1)) [0]
                            (:xf.clause (meta rel2)) [1]}
          ;merge prev and next index, keeping prev updated values
          rel-index-next   (merge-with (fn [prev _] prev) rel-index-prev' rel-index-next)]
      (-> rel1+rel2-v
        (vary-meta (fn [m] (assoc m :rel.index rel-index-next)))
        (update 0 (fn [x] (vary-meta x #(dissoc % :rel.index))))
        (update 1 (fn [x] (vary-meta x #(dissoc % :rel.index))))))
    rel1+rel2-v))

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
