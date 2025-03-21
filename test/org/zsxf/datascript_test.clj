(ns org.zsxf.datascript-test
  (:require
   [clojure.test :refer [deftest is]]
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [datascript.core :as d]
   [org.zsxf.datascript :as subject]
   [org.zsxf.experimental.datastream :as ds])
  (:import
   [java.io PushbackReader]))


(defn load-edn-file [file-path]
  (with-open [rdr (io/reader file-path)]
    (edn/read (PushbackReader. rdr))))
(def ra (atom []))
(defn load-learn-db []
  (let [schema (load-edn-file "resources/learndatalogtoday/schema_datascript.edn")
        data (load-edn-file   "resources/learndatalogtoday/data_datascript.edn")
        conn (d/create-conn schema)]
    (ds/listen-datom-stream conn ra)
    (d/transact! conn data)
    conn))

(comment [:find ?name
   :where
   [?p :person/name ?name] ;c1
   [?m :movie/title "RoboCop"] ;c2
   [?m :movie/director ?p] ;c3
   ]

;c1 unifies to c3 by ?p
;c2 unifies to c3 by ?p

;so that means we are joining c1 to c3 and c2 to c3?2
         )


#_(deftest defn-test
  (is (= true
         (subject/foo))))
#_(load-learn-db)
