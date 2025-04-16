(ns org.zsxf.datascript-test
  (:require
   [clojure.test :refer [deftest is]]
   [datascript.core :as d]
   [net.cgrand.xforms :as xforms]
   [org.zsxf.datascript :as ds]
   [org.zsxf.datalog.compiler :refer [sprinkle-dbsp-on]]
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
                           (sprinkle-dbsp-on [~@query]))]
     (ds/init-query-with-conn query# conn#)
     (is (= (q/get-result query#)
            (d/q (quote ~query) @conn#)
            ~result))))

(defmacro test-query-matches-db [query]
  `(let [conn#           (load-learn-db)
         query#           (q/create-query
                           (sprinkle-dbsp-on [~@query]))]
     (ds/init-query-with-conn query# conn#)
     (is (= (q/get-result query#)
            (d/q (quote ~query) @conn#)))))

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

(deftest same-name "basic predicate query"
  (test-query-gives-result [:find ?name
                            :where
                            [?p1 :person/name ?name]
                            [(= ?name "Sigourney Weaver")]]
                           #{["Sigourney Weaver"]}))

(deftest test-danny-pred "query with predicate"
  (test-query-matches-db [:find ?actor
                          :where
                          [?danny :person/name "Danny Glover"]
                          [?danny :person/born ?danny-born]
                          [?a :person/name ?actor]
                          [?a :person/born ?actor-born]
                          [_ :movie/cast ?a]
                          [(< ?actor-born ?danny-born)]]))

(comment
  (set! *print-meta* false)

  (sprinkle-dbsp-on [:find ?actor
                     :where
                     [?danny :person/name "Danny Glover"]
                     [?danny :person/born ?danny-born]
                     [?a :person/name ?actor]
                     [?a :person/born ?actor-born]
                     [_ :movie/cast ?a]
                     [(< ?actor-born ?danny-born)]])

  (def dxf (fn [state25590]
     (comp
      (xf/mapcat-zset-transaction-xf)
      (map
       (fn [zset__24931__auto__]
         (xf/disj-irrelevant-items
          zset__24931__auto__
          #(org.zsxf.datom2/datom-attr-val=
            %
            :person/name
            "Danny Glover")
          #(org.zsxf.datom2/datom-attr= % :person/born)
          #(org.zsxf.datom2/datom-attr= % :person/name)
          #(org.zsxf.datom2/datom-attr= % :movie/cast))))
      (xf/join-xf
       {:path identity,
        :pred #(org.zsxf.datom2/datom-attr= % :person/name),
        :index-kfn org.zsxf.datom2/datom->eid,
        :clause '[?a :person/name ?actor]}
       {:path identity,
        :pred #(org.zsxf.datom2/datom-attr= % :person/born),
        :index-kfn org.zsxf.datom2/datom->eid,
        :clause '[?a :person/born ?actor-born]}
       state25590)
      (xf/join-xf
       {:path org.zsxf.datalog.compiler/safe-first,
        :pred #(org.zsxf.datom2/datom-attr= % :person/name),
        :index-kfn org.zsxf.datom2/datom->eid,
        :clause '[?a :person/name ?actor]}
       {:path identity,
        :pred #(org.zsxf.datom2/datom-attr= % :movie/cast),
        :index-kfn org.zsxf.datom2/datom->val,
        :clause '[_ :movie/cast ?a]}
       state25590)
      (xf/join-xf
       {:path identity,
        :pred #(org.zsxf.datom2/datom-attr= % :person/born),
        :index-kfn org.zsxf.datom2/datom->eid,
        :clause '[?danny :person/born ?danny-born]}
       {:path identity,
        :pred
        #(org.zsxf.datom2/datom-attr-val=
          %
          :person/name
          "Danny Glover"),
        :index-kfn org.zsxf.datom2/datom->eid,
        :clause '[?danny :person/name "Danny Glover"]}
       state25590)
      (xf/cartesian-xf
       {:path
        (comp
         org.zsxf.datalog.compiler/safe-first
         org.zsxf.datalog.compiler/safe-first),
        :pred #(org.zsxf.datom2/datom-attr= % :person/name),
        :clause '[?a :person/name ?actor]}
       {:path org.zsxf.datalog.compiler/safe-first,
        :pred #(org.zsxf.datom2/datom-attr= % :person/born),
        :clause '[?danny :person/born ?danny-born]}
       state25590
       :last?
       true
       :return-zset-item-xf
       (comp
        (filter
         (fn [zset-item25591]
           (< ;; change this to .before and it works!
            ((comp
              org.zsxf.datom2/datom->val
              org.zsxf.datalog.compiler/safe-second
              org.zsxf.datalog.compiler/safe-first
              org.zsxf.datalog.compiler/safe-first)
             zset-item25591)
            ((comp
              org.zsxf.datom2/datom->val
              org.zsxf.datalog.compiler/safe-first
              org.zsxf.datalog.compiler/safe-second)
             zset-item25591))))))
      (xforms/reduce
       (zs/zset-xf+
        (map
         (xf/with-meta-f
           (juxt
            (comp
             org.zsxf.datom2/datom->val
             org.zsxf.datalog.compiler/safe-first
             org.zsxf.datalog.compiler/safe-first
             org.zsxf.datalog.compiler/safe-first)))))))))

  (let [conn           (load-learn-db)
        query           (q/create-query dxf)]
    (ds/init-query-with-conn query conn)
    (q/get-result query))



  )
