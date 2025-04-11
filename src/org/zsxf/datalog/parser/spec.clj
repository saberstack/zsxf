(ns org.zsxf.datalog.parser.spec
  (:require
   [clojure.spec.gen.alpha :as gen]
   [clojure.spec.alpha :as s]))

;; This whole ns is Claude-generated and may or may not be accurate
;; Helper macros for more concise spec definitions
(defmacro |
  "Alternative spec combinator, using keywords derived from symbol names"
  [& ks]
  `(s/alt ~@(interleave (map (comp keyword name) ks) ks)))

(defmacro &&
  "Sequential spec combinator with automatic keyword derivation"
  [& forms]
  `(s/cat ~@(for [form forms
                  :let [t (cond
                            (keyword? form) (keyword (name form))
                            (or (coll? form)
                                (symbol? form))
                            (if-let [t (->> form
                                            meta
                                            keys
                                            (filter (fn [k] (= (namespace k) (str *ns*))))
                                            first)]
                              (keyword (name t))
                              :_)
                            :else :_)]
                  x [t form]]
              x)))

;; (defmacro &&* [& forms] `(s/spec (&& ~@forms)))

;; Top-level query structure
(s/def ::query (&& ::find-spec ::where-clauses?))
;(s/def ::query (&& ::find-spec ::with-clause? ::inputs? ::where-clauses? ::return-map?))

;; Optional clauses
;; (s/def ::with-clause? (s/? ::with-clause))
;; (s/def ::inputs? (s/? ::inputs))
(s/def ::where-clauses? (s/? ::where-clauses))
;; (s/def ::return-map? (s/? ::return-map))

;; Find spec with variants
(s/def ::find-spec (&& #{:find} ^::find (| ::find-rel)))
;(s/def ::find-spec (&& #{:find} ^::find (| ::find-rel ::find-coll ::find-tuple ::find-scalar)))

;; Find variants
(s/def ::find-rel (s/+ ::find-elem))
;; (s/def ::find-coll (&&* ::find-elem #{'...}))
;; (s/def ::find-scalar (&& ::find-elem #{'.}))
;; (s/def ::find-tuple (s/spec (s/+ ::find-elem)))

;; Find elements
(s/def ::find-elem (| ::variable ))
;; (s/def ::find-elem (| ::variable ::pull-expr ::aggregate ::aggregate-custom))

;; Aggregate functions
;; (s/def ::aggregate (&&* (| ::plain-symbol ::variable) ::fn-args))
;; (s/def ::aggregate-custom (&&* #{'aggregate} ::variable ::fn-args))

;; Pull expressions
;; (s/def ::pull-expr (&&* #{'pull} ::src-var? ::variable ::pull-pattern))
;; (s/def ::pull-pattern (| ::variable ::plain-symbol ::constant))

;; Return map variants
;; (s/def ::return-map (| ::return-keys ::return-syms ::return-strs))
;; (s/def ::return-keys (&& #{:keys} ^::return (s/+ symbol?)))
;; (s/def ::return-syms (&& #{:syms} ^::return (s/+ symbol?)))
;; (s/def ::return-strs (&& #{:strs} ^::return (s/+ symbol?)))

;; With clause
;; (s/def ::with-clause (&& #{:with} (s/+ ::variable)))

;; Where clause
(s/def ::where-clauses (&& #{:where} (s/+ ::clause)))

;; Inputs
;; (s/def ::inputs (&& #{:in} (s/+ ::in-binding)))
;; (s/def ::in-binding (| ::src-var ::rules-var ::plain-variable ::binding))

;; Clauses
(s/def ::clause (| ::pattern; ::predicate ::function ::rule-expr
                   ;::not-clause ::not-join-clause
                   ;::or-clause ::or-join-clause
                   ;::and-clause
                   ))

;; Basic types
(s/def ::variable (s/with-gen
                    (s/and simple-symbol? #(= \? (first (name %))))
                    (fn [] (gen/fmap #(symbol (str "?" (name %))) (gen/symbol)))))

;; (s/def ::src-var (s/with-gen
;;                    (s/and simple-symbol? #(= \$ (first (name %))))
;;                    (fn [] (gen/fmap #(symbol (str "$" (name %))) (gen/symbol)))))

;; (s/def ::default-src #(instance? datascript.parser.DefaultSrc %))

;; (s/def ::src-var? (s/? ::src-var))

;; (s/def ::rules-var #{'%})

(s/def ::plain-symbol (s/and simple-symbol?
                             #(not= '_ %)
                             #(not (#{\$ \?} (first (name %))))))

(s/def ::plain-variable ::plain-symbol)

(s/def ::placeholder #{'_})

;; Constants
(s/def ::constant
  (s/or :number number?
        :string string?
        :boolean boolean?
        :keyword keyword?
        :set (s/and set? (s/coll-of ::constant :into #{}))
        :vector (s/and vector? (s/coll-of ::constant :into []))
        :list (s/and list? (s/coll-of ::constant :into ()))
        :map (s/map-of ::constant ::constant)))

;; Pattern elements and data patterns
(s/def ::pattern-el (| ::variable ::constant ::placeholder))
(s/def ::pattern (s/spec (s/cat :entity ::pattern-el
                                :attribute ::pattern-el
                                :value ::pattern-el)))
 ;(s/def ::pattern (&&* ::src-var? (s/+ ::pattern-el)))

;; Function arguments
;; (s/def ::fn-arg (| ::variable ::src-var ::constant))
;; (s/def ::fn-args (s/+ ::fn-arg))

;; Predicates and Functions
;; (s/def ::predicate (&&* (&&* (| ::plain-symbol ::variable) ::fn-args)))
;; (s/def ::function (&&* (&&* (| ::plain-symbol ::variable) ::fn-args) ::binding))

;; Rule expressions
;; (s/def ::rule-expr (&&* ::src-var? ::plain-symbol (s/+ ::pattern-el)))

;; Complex clauses
;; (s/def ::not-clause (&&* ::src-var? #{'not} (s/+ ::clause)))
;; (s/def ::not-join-clause (&&* ::src-var? #{'not-join} ^::variables (s/spec (s/+ ::variable)) (s/+ ::clause)))
;; (s/def ::or-clause (&&* ::src-var? #{'or} (s/+ (| ::clause ::and-clause))))
;; (s/def ::and-clause (&&* #{'and} (s/+ ::clause)))
;; (s/def ::or-join-clause (&&* ::src-var? #{'or-join} ::rule-vars (s/+ (| ::clause ::and-clause))))

;; Rule vars - required and free variables
;; (s/def ::rule-vars (s/spec
;;                      (s/alt :required-only (s/+ ::variable)
;;                             :required-and-free (s/cat :required (s/spec (s/+ ::variable))
;;                                                      :free (s/* ::variable)))))

;; Bindings for inputs
;; (s/def ::binding (| ::bind-scalar ::bind-tuple ::bind-coll ::bind-rel ::bind-ignore))
;; (s/def ::bind-ignore ::placeholder)
;; (s/def ::bind-scalar ::variable)
;; (s/def ::bind-tuple (s/spec (s/+ (| ::binding ::bind-ignore))))
;; (s/def ::bind-coll (&&* ::binding #{'...}))
;; (s/def ::bind-rel (s/spec [::bind-tuple]))

;; Rules definitions
;; (s/def ::rule-branch (&&* ::rule-vars (s/+ ::clause)))
;; (s/def ::rule (&&* ::plain-symbol (s/+ ::rule-branch)))
;; (s/def ::rules (s/+ ::rule))
 ;; # Top-level query structure
; formal grammar (Claude-generated; may not be accurate)
;; Query ::= [:find FindSpec
;;            [:with WithVars]?
;;            [:in InputSources]?
;;            [:where Clauses]?
;;            [:limit Limit]?
;;            [:offset Offset]?
;;            [:return-maps ReturnMaps]?]

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
