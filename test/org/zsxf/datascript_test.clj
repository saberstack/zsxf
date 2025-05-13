(ns org.zsxf.datascript-test
  (:require
   [clojure.test :refer [deftest is]]
   [datascript.core :as d]
   [net.cgrand.xforms :as xforms]
   [org.zsxf.datascript :as ds]
   [org.zsxf.datalog.compiler :refer [static-compile]]
   [org.zsxf.query :as q]
   [org.zsxf.util :as util]
   [org.zsxf.xf :as xf]
   [org.zsxf.zset :as zs]))

(defn load-learn-db
  []
  (let [schema (util/read-edn-file "resources/learndatalogtoday/schema_datascript.edn")
        data (util/read-edn-file   "resources/learndatalogtoday/data_datascript.edn")
        conn (d/create-conn schema)]
    (d/transact! conn data)
    conn))

(defmacro test-query-gives-result [query result]
  `(let [conn#           (load-learn-db)
         query#           (q/create-query
                           (static-compile [~@query]))]
     (ds/init-query-with-conn query# conn#)
     (is (= (q/get-result query#)
            (d/q (quote ~query) @conn#)
            ~result))))

(defmacro test-query-matches-db [query]
  `(let [conn#           (load-learn-db)
         query#           (q/create-query
                           (static-compile [~@query]))]
     (ds/init-query-with-conn query# conn#)
     (is (= (q/get-result query#)
            (d/q (quote ~query) @conn#)))))

(deftest single-clause-query "Special case with just one clause"
  (test-query-gives-result [:find ?value
                          :where [1 :person/name ?value]]
                           #{["James Cameron"]}))

(deftest test-robocop-with-query-api "basic datalog query, with internal query api"
  (test-query-gives-result [:find ?name
                            :where
                            [?p :person/name ?name]
                            [?m :movie/title "RoboCop"]
                            [?m :movie/director ?p]]
                           #{["Paul Verhoeven"]}))

(deftest test-ahhnold "Another basic query"
  (test-query-gives-result
   [:find ?name
    :where
    [?m :movie/cast ?p]
    [?p :person/name "Arnold Schwarzenegger"]
    [?m :movie/director ?d]
    [?d :person/name ?name]]
   #{["Mark L. Lester"]
     ["Jonathan Mostow"]
     ["John McTiernan"]
     ["James Cameron"]}))

(deftest test-b "another ad-hoc query"
  (test-query-gives-result
   [:find ?title ?year
    :where
    [?m :movie/title ?title]
    [?m :movie/year ?year]]
   #{["Lethal Weapon" 1987] ["Aliens" 1986]
     ["The Terminator" 1984] ["Rambo: First Blood Part II" 1985]
     ["Mad Max Beyond Thunderdome" 1985] ["Mad Max" 1979]
     ["First Blood" 1982] ["Predator" 1987]
     ["Terminator 2: Judgment Day" 1991] ["Predator 2" 1990]
     ["Mad Max 2" 1981] ["Lethal Weapon 2" 1989]
     ["Braveheart" 1995] ["Terminator 3: Rise of the Machines" 2003]
     ["Commando" 1985] ["Die Hard" 1988]
     ["Alien" 1979] ["RoboCop" 1987]
     ["Rambo III" 1988] ["Lethal Weapon 3" 1992]}))

(deftest test-danny-cartesian "query with cartesian product"
  (test-query-matches-db [:find ?danny-born ?actor-born
                          :where
                          [?danny :person/name "Danny Glover"]
                          [?danny :person/born ?danny-born]
                          [?a :person/name ?actor]
                          [?a :person/born ?actor-born]
                          [_ :movie/cast ?a]]))

(deftest test-danny-pred "query with predicate"
  (test-query-gives-result [:find ?actor
                            :where
                            [?danny :person/name "Danny Glover"]
                            [?danny :person/born ?danny-born]
                            [?a :person/name ?actor]
                            [?a :person/born ?actor-born]
                            [_ :movie/cast ?a]
                            [(< ?actor-born ?danny-born)]]
                           #{["Joe Pesci"] ["Brian Dennehy"] ["Tom Skerritt"]
                             ["Alan Rickman"] ["Tina Turner"] ["Bruce Spence"]
                             ["Michael Preston"] ["Charles Napier"] ["Gary Busey"]
                             ["Sylvester Stallone"] ["Ronny Cox"] ["Richard Crenna"]}))

(comment
  (set! *print-meta* false)

  )
