(ns org.zsxf.test-data.movielens
  (:require [org.zsxf.test-data.movielens-etl :as etl]
            [org.zsxf.datalog.compiler :refer [sprinkle-dbsp-on]]
            [org.zsxf.datascript :as ds]
            [org.zsxf.query :as q]
            [datascript.core :as d]))

;; Note: goes OOM on both datascript and on zsxf
;; The graph branches out very fast, so any algorithm would need to be clever here.
(def danny-4 (q/create-query
              (sprinkle-dbsp-on [:find ?a3-name ?m3-name ?a2-name ?m2-name ?a1-name ?m1-name ?a0-name ?m0-name ?danny-name
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
(def danny-1 (q/create-query
              (sprinkle-dbsp-on [:find ?a0-name ?m0-name ?danny-name
                                 :where
                                 [?danny :actor/name "Danny Glover"]
                                 [?m0 :movie/cast ?danny]
                                 [?m0 :movie/cast ?actor-0]
                                 [?m0 :movie/title ?m0-name]
                                 [?danny :actor/name ?danny-name]
                                 [?actor-0 :actor/name ?a0-name]
                                 [(clojure.core/distinct? ?danny ?actor-0 )]])))


;; What movies has Danny appeared in?
(def danny-0 (q/create-query
              (sprinkle-dbsp-on [:find ?m0-name ?danny-name
                                 :where
                                 [?danny :actor/name "Danny Glover"]
                                 [?m0 :movie/cast ?danny]
                                 [?m0 :movie/title ?m0-name]
                                 [?danny :actor/name ?danny-name]
                                 [?actor-0 :actor/name ?a0-name]])))


;; Simple join of all cast in the DB
(def all-cast (q/create-query
               (sprinkle-dbsp-on
                [:find ?title ?name
                 :where
                 [?m :movie/title ?title]
                 [?m :movie/cast ?a]
                 [?a :actor/name ?name]])))

(comment

  (def conn (etl/fresh-conn))
  (etl/populate-datascript-db conn)

  ; Size of db
  (for [attribute [:movie/title :movie/cast :actor/name]]
    (count (d/datoms @conn :aevt attribute))); (45319 560550 206158)

  ;; Danny 1
  [:find ?a0-name ?m0-name ?danny-name
   :where
   [?danny :actor/name "Danny Glover"]
   [?m0 :movie/cast ?danny]
   [?m0 :movie/cast ?actor-0]
   [?m0 :movie/title ?m0-name]
   [?danny :actor/name ?danny-name]
   [?actor-0 :actor/name ?a0-name]
   [(clojure.core/distinct? ?danny ?actor-0 )]]


  ;; 58 milliseconds
  (time (d/q '[:find ?a0-name ?m0-name ?danny-name
                :where
                [?danny :actor/name "Danny Glover"]
                [?m0 :movie/cast ?danny]
                [?m0 :movie/cast ?actor-0]
                [?m0 :movie/title ?m0-name]
                [?danny :actor/name ?danny-name]
                [?actor-0 :actor/name ?a0-name]
                [(clojure.core/distinct? ?danny ?actor-0 )]]
              @conn))
  ;; 210 seconds
  (time (ds/init-query-with-conn danny-1 conn))

  ;; 30 nanoseconds
  (time (q/get-result danny-1))


  ;; ALL CAST

  [:find ?title ?name
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
