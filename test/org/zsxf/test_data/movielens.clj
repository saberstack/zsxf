(ns org.zsxf.test-data.movielens
  (:require [clj-memory-meter.core :as mm]
            [medley.core :as medley]
            [org.zsxf.test-data.movielens-etl :as etl]
            [org.zsxf.datalog.compiler :refer [static-compile]]
            [org.zsxf.input.datascript :as ds]
            [org.zsxf.query :as q]
            [datascript.core :as d]
            [org.zsxf.util :as util]
            [taoensso.timbre :as timbre]))

;; Note: goes OOM on both datascript and on zsxf
;; The graph branches out very fast, so any algorithm would need to be clever here.
#_(def danny-4 (q/create-query
                 (static-compile '[:find ?a3-name ?m3-name ?a2-name ?m2-name ?a1-name ?m1-name ?a0-name ?m0-name ?danny-name
                                   :where
                                   [?danny :actor/name "Danny Glover"]
                                   [?m0 :movie/cast ?danny]
                                   [?m0 :movie/cast ?actor-0]
                                   [?m1 :movie/cast ?actor-0]
                                   [?m1 :movie/cast ?actor-1]
                                   [?m2 :movie/cast ?actor-1]
                                   [?m2 :movie/cast ?actor-2]
                                   [?m3 :movie/cast ?actor-2]
                                   [?m3 :movie/cast ?actor-3]
                                   [?m0 :movie/title ?m0-name]
                                   [?m1 :movie/title ?m1-name]
                                   [?m2 :movie/title ?m2-name]
                                   [?m3 :movie/title ?m3-name]
                                   [?danny :actor/name ?danny-name]
                                   [?actor-0 :actor/name ?a0-name]
                                   [?actor-1 :actor/name ?a1-name]
                                   [?actor-2 :actor/name ?a2-name]
                                   [?actor-3 :actor/name ?a3-name]
                                   [(clojure.core/distinct? ?danny ?actor-0 ?actor-1 ?actor-2 ?actor-3)]
                                   [(clojure.core/distinct? ?m0 ?m1 ?m2 ?m3)]])))

;; What was the cast that worked with Danny on all of his movies?
(def danny-1-query
  '[:find ?a-name
    :where
    ;[?m :movie/cast ?danny]
    ;[?danny :actor/name "Danny Glover"]
    [?m :movie/cast ?actor]
    [?actor :actor/name ?a-name]
    [?m :movie/title "Iron Man"]])


;; What movies has Danny appeared in?
#_(def danny-0 (q/create-query
                 (static-compile [:find ?m0-name ?danny-name
                                  :where
                                  [?danny :actor/name "Danny Glover"]
                                  [?m0 :movie/cast ?danny]
                                  [?m0 :movie/title ?m0-name]
                                  [?danny :actor/name ?danny-name]
                                  [?actor-0 :actor/name ?a0-name]])))

(def danny-0
  (q/create-query
    (static-compile
      '[:find ?m0-name
        :where
        [?danny :actor/name "Danny Glover"]
        [?m0 :movie/cast ?danny]
        [?m0 :movie/title ?m0-name]])))

(def iron-man-query-ds
  '[:find ?eid
    :where
    [?eid :movie/title "The Terminator"]])

(def movie-query-ds
  '[:find ?a-name
    :where
    ;[?m0 :movie/cast ?danny]
    ;[?danny :actor/name "Danny Glover"]
    [?m :movie/title "The Terminator"]
    [?actor :actor/name ?a-name]
    [?m :movie/cast ?actor]])

(comment
  (time (ds/init-query-with-conn danny-0 conn))
  (mm/measure danny-0))


;; Simple join of all cast in the DB
(def all-cast
  (q/create-query
    (static-compile
      '[:find ?title ?name
        :where
        [?m :movie/title ?title]
        [?m :movie/cast ?a]
        [?a :actor/name ?name]])))

(defn init-data []
  (def conn (d/create-conn etl/schema))
  (time
    (do
      (etl/populate-datascript-db conn)
      :done)))

(comment

  (init-data)

  (dx/inspect "movie-conn" conn)

  (d/q iron-man-query-ds @conn)

  (time (d/q movie-query-ds @conn))

  (mm/measure conn)

  (ds/unlisten-all! conn)

  (let [iron-man-lg (q/create-query
                      (static-compile
                        '[:find ?a-name
                          :where
                          ;[?m0 :movie/cast ?danny]
                          ;[?danny :actor/name "Danny Glover"]
                          [?m :movie/title "The Terminator"]
                          [?m :movie/cast ?actor]
                          [?actor :actor/name ?a-name]])
                      )
        iron-man-sm (q/create-query
                      (static-compile
                        '[:find ?a-name
                          :where
                          ;[?m0 :movie/cast ?danny]
                          ;[?danny :actor/name "Danny Glover"]
                          [?m :movie/cast ?actor]
                          [?actor :actor/name ?a-name]
                          [?m :movie/title "The Terminator"]])
                      )

        iron-man-3  (q/create-query
                      (static-compile
                        '[:find ?a-name
                          :where
                          ;[?m0 :movie/cast ?danny]
                          ;[?danny :actor/name "Danny Glover"]
                          [?actor :actor/name ?a-name]
                          [?m :movie/cast ?actor]
                          [?m :movie/title "The Terminator"]])
                      )]

    (time (ds/init-query-with-conn iron-man-sm conn))
    (time (ds/init-query-with-conn iron-man-lg conn))
    (time (ds/init-query-with-conn iron-man-3 conn))

    (def iron-man-lg iron-man-lg)
    (def iron-man-sm iron-man-sm)
    (def iron-man-3 iron-man-3)


    [(mm/measure iron-man-lg)
     (mm/measure iron-man-sm)
     (mm/measure iron-man-3)]

    )
  (mm/measure [conn iron-man-3])
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;










(comment
  ; Size of db
  (for [attribute [:movie/title :movie/cast :actor/name]]
    (count (d/datoms @conn :aevt attribute)))               ; (45319 560550 206158)

  ;; Danny 1
  (d/q
    '[:find ?a0-name ?m0-name ?danny-name
      :where
      [?danny :actor/name "Danny Glover"]
      [?m0 :movie/cast ?danny]
      [?m0 :movie/cast ?actor-0]
      [?m0 :movie/title ?m0-name]
      [?danny :actor/name ?danny-name]
      [?actor-0 :actor/name ?a0-name]
      [(clojure.core/distinct? ?danny ?actor-0)]]
    @conn)


  ;; 58 milliseconds
  (time (d/q '[:find ?a0-name ?m0-name ?danny-name
               :where
               [?danny :actor/name "Danny Glover"]
               [?m0 :movie/cast ?danny]
               [?m0 :movie/cast ?actor-0]
               [?m0 :movie/title ?m0-name]
               [?danny :actor/name ?danny-name]
               [?actor-0 :actor/name ?a0-name]
               [(clojure.core/distinct? ?danny ?actor-0)]]
          @conn))

  (time (ds/init-query-with-conn danny-1 conn))
  (time (ds/init-query-with-conn danny-1b conn))
  (time
    (do
      (d/q danny-1-query @conn)
      :done))
  (= (q/get-result danny-1) (d/q danny-1-query @conn))



  (mm/measure conn)
  (mm/measure [conn danny-1])
  (mm/measure [conn danny-1b])

  (time
    (do
      (q/get-result danny-1)
      :done))


  ;; ALL CAST
  '[:find ?title ?name
    :where
    [?m :movie/title ?title]
    [?m :movie/cast ?a]
    [?a :actor/name ?name]]

  ;; 500-2300 milliseconds
  (time (d/q '[:find ?title ?name
               :where
               [?m :movie/title ?title]
               [?m :movie/cast ?a]
               [?a :actor/name ?name]]
          @conn))
  ;; 170 seconds
  (time (ds/init-query-with-conn all-cast conn))
  ;; 30-45 nanoseconds
  (time (q/get-result all-cast))

  )
