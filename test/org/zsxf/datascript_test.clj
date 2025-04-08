(ns org.zsxf.datascript-test
  (:require
   [clojure.test :refer [deftest is]]
   [datascript.core :as d]
   [org.zsxf.datascript :as ds]
   [org.zsxf.datalog.compiler :refer [sprinkle-dbsp-on]]
   [org.zsxf.query :as q]
   [org.zsxf.util :as util :refer [nth2]]
   [org.zsxf.experimental.datastream :as data-stream]
   [taoensso.timbre :as timbre]))

(defn load-learn-db
  ([]
   (load-learn-db nil))
  ([listen-atom]
   (let [schema (util/read-edn-file "resources/learndatalogtoday/schema_datascript.edn")
         data (util/read-edn-file   "resources/learndatalogtoday/data_datascript.edn")
         conn (d/create-conn schema)]
     (when listen-atom
       (data-stream/listen-datom-stream conn listen-atom ds/tx-datoms->datoms2->zset))
     (d/transact! conn data)
     conn)))

(deftest test-robocop-with-query-api "basic datalog query, with internal query api"
  (let [txn-atom (atom [])
        _conn           (load-learn-db txn-atom)
        query           (q/create-query
                         (sprinkle-dbsp-on [:find ?name
                                            :where
                                            [?p :person/name ?name]
                                            [?m :movie/title "RoboCop"]
                                            [?m :movie/director ?p]]))]
    (is (= (q/input query @txn-atom)
           (q/get-result query)
           #{["Paul Verhoeven"]}))))

(deftest test-ahhnold "Another basic query"
  (let [txn-atom (atom [])
        _conn (load-learn-db txn-atom)
        query (q/create-query
               (sprinkle-dbsp-on
                [:find ?name
                 :where
                 [?m :movie/cast ?p]
                 [?p :person/name "Arnold Schwarzenegger"]
                 [?m :movie/director ?d]
                 [?d :person/name ?name]]))]
    (is (= #{["Mark L. Lester"]
             ["Jonathan Mostow"]
             ["John McTiernan"]
             ["James Cameron"]}
           (q/input query @txn-atom)))))

(deftest test-b "another ad-hoc query"
  (let [conn   (load-learn-db)
        query  (q/create-query
                 (sprinkle-dbsp-on [:find ?title ?year
                                    :where
                                    [?m :movie/title ?title]
                                    [?m :movie/year ?year]]))]
    (ds/init-query-with-conn query conn)
    (is
        (= (q/get-result query)
           (d/q '[:find ?title ?year
                  :where
                  [?m :movie/title ?title]
                  [?m :movie/year ?year]]
                @conn)
                #{["Lethal Weapon" 1987] ["Aliens" 1986]
                  ["The Terminator" 1984] ["Rambo: First Blood Part II" 1985]
                  ["Mad Max Beyond Thunderdome" 1985] ["Mad Max" 1979]
                  ["First Blood" 1982] ["Predator" 1987]
                  ["Terminator 2: Judgment Day" 1991] ["Predator 2" 1990]
                  ["Mad Max 2" 1981] ["Lethal Weapon 2" 1989]
                  ["Braveheart" 1995] ["Terminator 3: Rise of the Machines" 2003]
                  ["Commando" 1985] ["Die Hard" 1988]
                  ["Alien" 1979] ["RoboCop" 1987]
                  ["Rambo III" 1988] ["Lethal Weapon 3" 1992]}))))

(comment
  (set! *print-meta* false)

  (let [conn  (load-learn-db)
        xf1 (sprinkle-dbsp-on [:find ?danny-born
                                   :where
                                   [?danny :person/name "Danny Glover"]
                                   [?danny :person/born ?danny-born]] )
        xf2 (sprinkle-dbsp-on [:find ?actor
                                   :where
                                   [?a :person/name ?actor]
                                   [?a :person/born ?actor-born]
                                   [_ :movie/cast ?a]] )
        xf (fn [state] (comp (xf2 state) (xf1 state)))

        query (q/create-query
                xf)
        ]
    (ds/init-query-with-conn query conn)
    #_(ds/init-query-with-conn query2 conn)
    (q/get-result query)
    )
  )
