(ns org.zsxf.datalog.parser.spec
  (:require
   #_[clojure.spec.gen.alpha :as gen]
   [clojure.spec.alpha :as s]
   [clojure.string]))


(s/def ::placeholder #{'_})

(s/def ::variable (s/and simple-symbol? #(= \? (first (name %)))))

(s/def ::constant
  (s/or :number number?
        :string string?
        :boolean boolean?
        :keyword keyword?
        :set (s/and set? (s/coll-of ::constant :into #{}))
        :vector (s/and vector? (s/coll-of ::constant :into []))
        :list (s/and list? (s/coll-of ::constant :into ()))
        :map (s/map-of ::constant ::constant)))

(s/def ::pattern-el (s/or :placeholder ::placeholder :variable ::variable :constant ::constant))

(s/def ::pattern (s/tuple ::pattern-el ::pattern-el ::pattern-el))

(s/def ::pred-operator #{'= 'not= '> '>= '< '<= 'clojure.core/distinct? 'clojure.string/includes?})
(s/def ::pred-operand (s/or :variable ::variable :constant ::constant))

(s/def ::predicate (s/tuple (s/cat :pred-operator ::pred-operator :pred-operands (s/+ ::pred-operand))))

(s/def ::clause (s/or :pattern ::pattern :predicate ::predicate))

(s/def ::where-clauses (s/cat :where-kw #{:where} :clauses (s/+ ::clause)))

(s/def ::aggregate (s/cat :aggregate-fn #{'sum 'count} :variable ::variable))

(s/def ::find-rel (s/+ (s/or :variable ::variable :aggregate ::aggregate)))

(s/def ::find-spec (s/cat :find-kw #{:find} :find-spec ::find-rel))

(s/def ::query (s/cat :find-spec ::find-spec :where ::where-clauses))

;; # Find specifications determine the shape of the results
;; FindSpec ::= FindRel | FindColl | FindTuple | FindScalar | PullExprs | AggregateExprs
;; FindRel ::= [Variable+]                            # Returns a relation (set of tuples)
;; FindColl ::= [[Variable+]]                         # Returns a collection of tuples
;; FindTuple ::= [Variable+ ...]                      # Returns a single tuple
;; FindScalar ::= Variable                            # Returns a single scalar value
;; PullExprs ::= [(pull Variable Pattern)+]           # Entity patterns to pull
;; AggregateExprs ::= [(AggregateFn Variable)+]       # Aggregated values

;; # With clause - variables to be used in :where but not returned
;; WithClause ::= :with [Variable+]

;; # In clause - data sources and external variables (default: [$])
;; InClause ::= :in [Binding+]
;; Binding ::= ScalarBinding | TupleBinding | CollBinding | RelationBinding
;; ScalarBinding ::= Variable | SrcVar | RulesVar
;; TupleBinding ::= [Variable+]
;; CollBinding ::= [[Variable]]
;; RelationBinding ::= [[[Variable+]]]

;; # Where clause - patterns that filter the results
;; WhereClause ::= :where [Clause+]
;; Clause ::= DataPattern | RuleClause | NotClause | OrClause | AndClause | PredClause | FnClause | NotJoinClause

;; # Data pattern is a basic triple/quad pattern [e a v] or [e a v tx]
;; DataPattern ::= [Entity Attribute Value+] | [SrcVar Entity Attribute Value+]
;; Entity ::= Variable | Constant | Placeholder
;; Attribute ::= Variable | Constant | Placeholder
;; Value ::= Variable | Constant | Placeholder

;; # Special forms
;; RuleClause ::= [RuleName Variable+] | [SrcVar RuleName Variable+]
;; NotClause ::= (not Clause+)
;; OrClause ::= (or Clause+)
;; AndClause ::= (and Clause+)
;; PredClause ::= [(Predicate Arg+)]           # Filter based on predicate
;; FnClause ::= [(Function Arg+) Variable]     # Bind result of function to variable
;; NotJoinClause ::= (not-join [Variable+] Clause+)
;; OrJoinClause ::= (or-join [Variable+] Clause+) | (or-join [[ReqVariable+] Variable*] Clause+)

;; # Rules definition
;; Rules ::= [Rule+]
;; Rule ::= [[RuleName Variable+] Clause+]

;; # Terminal values
;; Variable ::= Symbol starting with '?'        # Binds to a value
;; SrcVar ::= Symbol starting with '$'          # References a data source
;; RulesVar ::= '%'                             # References rules
;; Constant ::= Any value that is not a Variable, SrcVar, RulesVar, or Placeholder
;; Placeholder ::= '_'                          # Wildcard, matches anything but doesn't bind
