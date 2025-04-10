(ns org.zsxf.experimental.pull
  (:require [datascript.core :as d]))

(defn pull-join-xf []
  ; A pull pattern is similar to a join but differs in important ways.
  ; Given a query like:
  '[:find (pull ?p [:person/name
                    {:person/born [:country/name]}])
    :where
    [?p :person/name ?name]
    [?m :movie/title _]
    [?m :movie/director ?p]]
  ; ... we can see that the pull starts as typical at ?p (entity id)
  ; It _pulls_ the following:
  ;   :person/name  – which was already requested in the main :where join
  ;   :person/born  - new piece of data... which is actually a ref (a join!)
  ;      ... which points to ...
  ;   :country/name - new data also.
  ;
  ; (!) An important difference between a :where join and a (pull ...) "join":
  ;
  ; Even when a certain (pull ?p ...) "lacks" any (or all!) of the data requested by the (pull ...) pattern
  ; the number of returned items in the result does not change (the query can return a list like
  '([nil] [nil])                                            ; ... if nothing in the pattern is found
  ; or if found (more typically):
  '([#:person{:born #:country{:name "USA"} :name "Alice"}]
    [#:person{:born #:country{:name "Monaco"} :name "Bob"}])
  ;
  ; More important differences likely exist, this is WIP.
  ;
  )

(comment
  ;streams of streams WIP
  (eduction
    (map
      (fn [v]
        (eduction (map inc) v)))
    [[1 2 3]
     [4 5 6]])
  )
