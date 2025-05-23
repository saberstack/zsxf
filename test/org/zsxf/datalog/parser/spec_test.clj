(ns org.zsxf.datalog.parser.spec-test
  (:require [clojure.test :refer :all]
            [org.zsxf.datalog.parser.spec :as parser-spec]
            [clojure.spec.alpha :as s]))


(deftest variable-test
  (testing "Valid variables"
    (is (s/valid? ::parser-spec/variable '?x))
    (is (s/valid? ::parser-spec/variable '?name))
    (is (s/valid? ::parser-spec/variable '?person-name)))

  (testing "Invalid variables"
    (is (not (s/valid? ::parser-spec/variable 'x)))
    (is (not (s/valid? ::parser-spec/variable '$x)))
    (is (not (s/valid? ::parser-spec/variable '_)))
    (is (not (s/valid? ::parser-spec/variable 123)))
    (is (not (s/valid? ::parser-spec/variable :keyword)))))

(deftest placeholder-test
  (testing "Valid placeholder"
    (is (s/valid? ::parser-spec/placeholder '_)))

  (testing "Invalid placeholders"
    (is (not (s/valid? ::parser-spec/placeholder '_underscore)))
    (is (not (s/valid? ::parser-spec/placeholder '?name)))
    (is (not (s/valid? ::parser-spec/placeholder 'name)))))

(deftest constant-test
  (testing "Valid constants - primitive types"
    (is (s/valid? ::parser-spec/constant 42))
    (is (s/valid? ::parser-spec/constant "string"))
    (is (s/valid? ::parser-spec/constant true))
    (is (s/valid? ::parser-spec/constant :keyword))
    (is (s/valid? ::parser-spec/constant :namespace/keyword)))

  (testing "Valid constants - collection types"
    (is (s/valid? ::parser-spec/constant #{1 2 3}))
    (is (s/valid? ::parser-spec/constant [1 2 3]))
    (is (s/valid? ::parser-spec/constant '(1 2 3)))
    (is (s/valid? ::parser-spec/constant {:a 1 :b 2})))

  (testing "Valid constants - nested collections"
    (is (s/valid? ::parser-spec/constant {:a [1 2] :b #{3 4}}))
    (is (s/valid? ::parser-spec/constant [1 {:x "y"} #{:a :b}])))

  (testing "Invalid constants"
    (is (not (s/valid? ::parser-spec/constant '?var)))
    (is (not (s/valid? ::parser-spec/constant '_)))))

(deftest pattern-el-test
  (testing "Valid pattern elements"
    (is (s/explain-str ::parser-spec/pattern-el '?e))
    (is (s/valid? ::parser-spec/pattern-el :name))
    (is (s/valid? ::parser-spec/pattern-el 42))
    (is (s/valid? ::parser-spec/pattern-el '_)))

  (testing "Invalid pattern elements"
    (is (not (s/valid? ::parser-spec/pattern-el 'regular-symbol)))
    (is (not (s/valid? ::parser-spec/pattern-el '$src)))))

(deftest pattern-test
  (testing "Valid patterns"
    (is (s/valid? ::parser-spec/pattern '[?e :name "Alice"]))
    (is (s/valid? ::parser-spec/pattern '[?e :age 30]))
    (is (s/valid? ::parser-spec/pattern '[?e :friend ?friend]))
    (is (s/valid? ::parser-spec/pattern '[_ :type :person]))
    (is (s/valid? ::parser-spec/pattern '[123 :id ?id])))

  (testing "Invalid patterns - wrong structure"
    (is (not (s/valid? ::parser-spec/pattern '[])))
    (is (not (s/valid? ::parser-spec/pattern '[?e])))
    (is (not (s/valid? ::parser-spec/pattern '[?e :name])))
    (is (not (s/valid? ::parser-spec/pattern '[?e :name "Alice" :extra]))))

  (testing "Invalid patterns - wrong element types"
    (is (not (s/valid? ::parser-spec/pattern '[regular-symbol :name "Alice"])))
    (is (not (s/valid? ::parser-spec/pattern '[?e regular-symbol "Alice"])))
    (is (not (s/valid? ::parser-spec/pattern '[$src :name "Alice"])))))

;; Find element tests
(deftest find-elem-test
  (testing "Valid find elements (currently only variables)"
    (is (s/valid? ::parser-spec/variable '?name))
    (is (s/valid? ::parser-spec/variable '?person)))

  (testing "Invalid find elements"
    (is (not (s/valid? ::parser-spec/variable '_)))
    (is (not (s/valid? ::parser-spec/variable 'name)))
    (is (not (s/valid? ::parser-spec/variable 42)))
    (is (not (s/valid? ::parser-spec/variable :keyword)))))

(deftest find-rel-test
  (testing "Valid find relations"
    (is (s/valid? ::parser-spec/find-rel '(?name)))
    (is (s/valid? ::parser-spec/find-rel '(?person ?name)))
    (is (s/valid? ::parser-spec/find-rel '(?e ?a ?v))))

  (testing "Invalid find relations"
    (is (not (s/valid? ::parser-spec/find-rel '())))
    (is (not (s/valid? ::parser-spec/find-rel '(name))))
    (is (not (s/valid? ::parser-spec/find-rel '(?name name))))
    (is (not (s/valid? ::parser-spec/find-rel '(?name _))))))

(deftest find-spec-test
  (testing "Valid find specs"
    (is (s/valid? ::parser-spec/find-spec '(:find ?name)))
    (is (s/valid? ::parser-spec/find-spec '(:find ?person ?name))))

  (testing "Invalid find specs"
    (is (not (s/valid? ::parser-spec/find-spec '(:find))))
    (is (not (s/valid? ::parser-spec/find-spec '(:wrong ?name))))
    (is (not (s/valid? ::parser-spec/find-spec '(:find name))))
    (is (not (s/valid? ::parser-spec/find-spec '(:find ?name ...))))
    (is (not (s/valid? ::parser-spec/find-spec '(:find [?name]))))
    (is (not (s/valid? ::parser-spec/find-spec '(:find ?name .))))))

(deftest predicate-test
  (testing "valid predicates"
    (is (s/valid? ::parser-spec/predicate '[(> ?age 4)]))
    (is (s/valid? ::parser-spec/predicate '[(= 3 4)]))
    (is (s/valid? ::parser-spec/predicate '[(not= 3 "sheep")])))
  (testing "invalid predicates"
    (is (not (s/valid? ::parser-spec/predicate '(> ?age 4))))
    (is (not (s/valid? ::parser-spec/predicate '[(+ 3 4)])))
    (is (not (s/valid? ::parser-spec/predicate '[(?var ?var ?var)])))))

(deftest clause-test
  (testing "Valid clauses (patterns or predicates)"
    (is (s/valid? ::parser-spec/pattern '[?e :name "Alice"]))
    (is (s/valid? ::parser-spec/clause '[?e :name "Alice"]))
    (is (s/valid? ::parser-spec/pattern '[?product :price ?price]))
    (is (s/valid? ::parser-spec/pattern '[_ :type :person]))
    (is (s/valid? ::parser-spec/clause '[(> ?age 4)])))

  (testing "Invalid patterns"
    (is (not (s/valid? ::parser-spec/pattern '(not [?e :name "Alice"]))))
    (is (not (s/valid? ::parser-spec/pattern '[(< ?price 100) ?result])))
    (is (not (s/valid? ::parser-spec/pattern '[?e])))
    (is (not (s/valid? ::parser-spec/pattern '?variable)))))

(deftest where-clauses-test
  (testing "Valid where clauses"
    (is (s/valid? ::parser-spec/where-clauses '(:where [?e :name "Alice"])))
    (is (s/valid? ::parser-spec/where-clauses '(:where
                                                [?p :name ?name]
                                                [?p :age ?age])))
    (is (s/valid? ::parser-spec/where-clauses '(:where
                                                [?p :name ?name]
                                                [?p :friend ?friend]
                                                [?friend :name ?friend-name]
                                                [(not= ?name ?friend-name)]))))

  (testing "Invalid where clauses"
    (is (not (s/valid? ::parser-spec/where-clauses '(:where))))
    (is (not (s/valid? ::parser-spec/where-clauses '(:wrong [?e :name "Alice"]))))
    (is (not (s/valid? ::parser-spec/where-clauses '(:where ?e))))
    (is (not (s/valid? ::parser-spec/where-clauses '(:where [?e]))))
    (is (not (s/valid? ::parser-spec/where-clauses '(:where (not [?e :name "Alice"])))))))

;; Complete query tests
(deftest query-test
  (testing "Valid queries - minimal"
    (is (s/valid? ::parser-spec/query '[:find ?name
                                        :where [?p :name ?name]])))

  (testing "Valid queries - multiple patterns"
    (is (s/valid? ::parser-spec/query '[:find ?name
                                        :where
                                        [?p :name ?name]
                                        [?p :age ?age]])))

  (testing "Valid queries - multiple patterns with joins"
    (is (s/valid? ::parser-spec/query '[:find ?name
                                        :where
                                        [?p :name ?name]
                                        [?p :friend ?f]
                                        [?f :name ?friend-name]])))

  (testing "Valid queries - aggregate"
    (is (s/valid? ::parser-spec/query '[:find ?name (count ?name)
                                        :where [?p :name ?name]])))

  (testing "Valid queries - multiple find vars"
    (is (s/valid? ::parser-spec/query '[:find ?name ?age
                                        :where
                                        [?p :name ?name]
                                        [?p :age ?age]])))

  (testing "Invalid queries - missing required parts"
    (is (not (s/valid? ::parser-spec/query '[])))
    (is (not (s/valid? ::parser-spec/query '[:find])))
    (is (not (s/valid? ::parser-spec/query '[:find ?name])))
    (is (not (s/valid? ::parser-spec/query '[:where [?p :name ?name]]))))

  (testing "Invalid queries - wrong structure"
    (is (not (s/valid? ::parser-spec/query '[:find ?name :where])))
    (is (not (s/valid? ::parser-spec/query '[:find ?name :where [?p]]))))

  (testing "Invalid queries - wrong element types"
    (is (not (s/valid? ::parser-spec/query '[:find name :where [?p :name name]])))
    (is (not (s/valid? ::parser-spec/query '[:find ?name :where [p :name ?name]]))))

  (testing "Invalid queries - unsupported features (should fail)"
    (is (not (s/valid? ::parser-spec/query '[:find ?name
                                             :with ?p
                                             :where [?p :name ?name]])))
    (is (not (s/valid? ::parser-spec/query '[:find ?name
                                             :in ?search-name
                                             :where [?p :name ?search-name]])))
    (is (not (s/valid? ::parser-spec/query '[:find ?name
                                             :where
                                             [?p :name ?name]
                                             (not [?p :blocked true])])))
    (is (not (s/valid? ::parser-spec/query '[:find ?name ...
                                             :where [?p :name ?name]])))
    (is (not (s/valid? ::parser-spec/query '[:find ?name .
                                             :where [?p :name ?name]]))))

  (testing "Invalid queries - unsupported aggregate"
    (is (not (s/valid? ::parser-spec/query '[:find ?name (min ?height)
                                             :where
                                             [?p :person/name ?name]
                                             [?p :person/height ?height]])))))
