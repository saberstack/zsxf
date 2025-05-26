(ns org.zsxf.datascript-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datascript.core :as d]
   [net.cgrand.xforms :as xforms]
   [org.zsxf.datascript :as ds]
   [org.zsxf.datalog.compiler :refer [static-compile]]
   [org.zsxf.query :as q]
   [org.zsxf.util :as util]
   [org.zsxf.xf :as xf]
   [org.zsxf.zset :as zs]))

(defn load-test-db
  [dir]
  (let [schema (util/read-edn-file (format "resources/%s/schema_datascript.edn" dir))
        data (util/read-edn-file   (format "resources/%s/data_datascript.edn" dir))
        conn (d/create-conn schema)]
    (d/transact! conn data)
    conn))

(defmacro test-query-gives-result
  [dir query result]
   `(let [conn#           (load-test-db ~dir)
          query#           (q/create-query
                            (static-compile [~@query]))]
      (ds/init-query-with-conn query# conn#)
      (is (= (q/get-result query#)
             (d/q (quote ~query) @conn#)
             ~result))))

(defmacro test-query-matches-db
  [dir query]
   `(let [conn#           (load-test-db ~dir)
          query#           (q/create-query
                            (static-compile [~@query]))]
      (ds/init-query-with-conn query# conn#)
      (is (= (q/get-result query#)
             (d/q (quote ~query) @conn#)))))

(deftest single-clause-queries "Special case with just one clause"
  (testing "wildcard eid"
    (test-query-matches-db "learndatalogtoday"
     [:find ?value
      :where [_ :person/name ?value]]))
  (testing "eid literal"
    (test-query-gives-result "learndatalogtoday"
     [:find ?value
      :where [1 :person/name ?value]]
     #{["James Cameron"]}))
  (testing "eid literal with attr wildcard"
    (test-query-gives-result "learndatalogtoday"
     [:find ?value
      :where [1 _ ?value]]
     #{["James Cameron"]
       [#inst"1954-08-16"]}))
  (testing "all values in the db"
    (test-query-matches-db "learndatalogtoday"
     [:find ?value
      :where [_ _ ?value]])))

(deftest test-robocop-with-query-api "basic datalog query, with internal query api"
  (test-query-gives-result "learndatalogtoday"
                           [:find ?name
                            :where
                            [?p :person/name ?name]
                            [?m :movie/title "RoboCop"]
                            [?m :movie/director ?p]]
                           #{["Paul Verhoeven"]}))

(deftest test-ahhnold "Another basic query"
  (test-query-gives-result "learndatalogtoday"
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
  (test-query-gives-result "learndatalogtoday"
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
  (test-query-matches-db "learndatalogtoday"
                         [:find ?danny-born ?actor-born
                          :where
                          [?danny :person/name "Danny Glover"]
                          [?danny :person/born ?danny-born]
                          [?a :person/name ?actor]
                          [?a :person/born ?actor-born]
                          [_ :movie/cast ?a]]))

(deftest test-danny-pred "query with predicate"
  (test-query-gives-result "learndatalogtoday"
                           [:find ?actor
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

(deftest test-aggregates
  (let [conn (load-test-db "baseball-legends")
        q '[:find ?name (count ?year) (sum ?hits) (sum ?home-runs)
            :where
            [?p :player/name ?name]
            [?s :season/player ?p]
            [?s :season/year ?year]
            [?s :season/hits ?hits]
            [?s :season/home-runs ?home-runs]]
        query (q/create-query (static-compile [:find ?name (count ?year) (sum ?hits) (sum ?home-runs)
                                               :where
                                               [?p :player/name ?name]
                                               [?s :season/player ?p]
                                               [?s :season/year ?year]
                                               [?s :season/hits ?hits]
                                               [?s :season/home-runs ?home-runs]]))]

    (testing "career home runs and seasons played"
      (ds/init-query-with-conn query conn)
      (is (= [["Babe Ruth" 22 3035 714] ["Ted Williams" 19 2654 521] ["Mickey Mantle" 18 2491 536]]
             (d/q q @conn)))
      (is  (= {["Babe Ruth"] #{['?year 22] ['?home-runs 714] ['?hits 3035]}
               ["Ted Williams"] #{['?year 19] ['?hits 2654] ['?home-runs 521]}
               ["Mickey Mantle"]#{['?year 18] ['?hits 2491] ['?home-runs 536]}}
              (q/get-aggregate-result query))))
    (testing "an additional transaction doesn't break the query"
      (d/transact! conn [{:season/player 1 :season/year 1975 :season/hits 150 :season/home-runs 62 :season/at-bats 500}])
      (is  (= {["Babe Ruth"] #{['?year 23] ['?home-runs 776] ['?hits 3185]}
               ["Ted Williams"] #{['?year 19] ['?hits 2654] ['?home-runs 521]}
               ["Mickey Mantle"]#{['?year 18] ['?hits 2491] ['?home-runs 536]}}
              (q/get-aggregate-result query))))))

(comment
  (set! *print-meta* false)

  )
