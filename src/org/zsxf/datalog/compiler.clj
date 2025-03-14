(ns org.zsxf.datalog.compiler
  (:require [org.zsxf.xf :as dbsp-xf]
            [org.zsxf.zset :as zs]
            [org.zsxf.datalog.parser :as parser]
            [pangloss.transducers :as pxf]
            [pattern :as p]))

(defn init []
  (p/compile-pattern '[?before 1 2 3 ?after]))

(comment
  ((init) [0 1 2 3 4]))

(def m (p/compile-pattern '[?fn-name-1 (fn ?fn-name-2 [??args] ??body)]))

(m ['abc '(fn abc [x y z] x)])                              ; => {:fn-name 'abc ...}
(m ['xyz '(fn abc [x] x)])                                  ; => nil

(def example-datalog-query-1
  '[:find (pull ?exercise-set-eids [:db/id])
    :in $ ?workout-uuid ?person-eid
    :where
    [?person-eid :person/country "USA"]
    [?person-eid :person/workouts ?workout-eid]
    [?workout-eid :workout/uuid ?workout-uuid]
    [?exercise-set-eids :exercise-set/workout ?workout-eid]])


(comment
  (and
    [?person-eid :person/country "USA"]
    [?person-eid :person/workouts
     [?workout-eid :workout/uuid ?workout-uuid]])

  [?exercise-set-eids :exercise-set/workout
   [?workout-eid :workout/uuid ?workout-uuid]]

  )


(def datalog->zsxf-matcher
  (p/compile-pattern
    '[??_
      :where ??clauses]))

(defn datalog->zsxf [query]
  (datalog->zsxf-matcher query))

(def my-rule (p/rule '(+ ??x) (p/sub (- ??x))))

(comment
  (datalog->zsxf example-datalog-query-1)

  (let [x 2
        y '[a b c d]]
    (p/sub (* (+ ??y) ?x)))


  (my-rule '(+ 2 5 1)))

;TODO

; 1. convert datalog query to a DAG
; 2. ...
;
; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;

(defmacro join-one-level [attribute-1 attribute-2 value]
  `(pxf/branch
     ;:team/id (or :team/name, a unique attribute)
     (comp
       (map (fn [m#] (if (~attribute-2 m#) m# {})))
       (pxf/cond-branch
         empty?
         (map identity)
         ~attribute-2
         (comp
           (dbsp-xf/->where-xf (fn [m#] (= ~value (~attribute-2 m#))))
           (dbsp-xf/->index-xf ~attribute-2)
           ;atoms
           ; TODO
           ;(map (fn [grouped-by-result]
           ;       (swap! *grouped-by-state-team
           ;         (fn [m] (zs/indexed-zset+ m grouped-by-result)))))
           )))
     (comp
       ;:player/team
       (map (fn [m#] (if (~attribute-1 m#) m# {})))
       (pxf/cond-branch
         empty?
         (map identity)
         ~attribute-1
         (comp
           (dbsp-xf/->index-xf ~attribute-1)
           ;atoms
           ; TODO
           ;(map (fn [grouped-by-result]
           ;       (swap! *grouped-by-state-player
           ;         (fn [m] (zs/indexed-zset+ m grouped-by-result)))))
           )))))

(defn join-one-level-2 [keyword-1 atom-1 keyword-2 atom-2]
  (pxf/branch
    ;:team/id (or :team/name, a unique attribute)
    (comp
      (map (fn [m] (if (keyword-2 m) m {})))
      (pxf/cond-branch
        empty?
        (map identity)
        keyword-2
        (comp
          (dbsp-xf/->index-xf keyword-2)
          ;atoms
          (map (fn [grouped-by-result]
                 (swap! atom-1 (fn [m] (zs/indexed-zset+ m grouped-by-result)))))
          )))
    (comp
      ;:player/team
      (map (fn [m] (if (keyword-1 m) m {})))
      (pxf/cond-branch
        empty?
        (map identity)
        keyword-1
        (comp
          (dbsp-xf/->index-xf keyword-1)
          ;atoms
          (map (fn [grouped-by-result]
                 (swap! atom-2 (fn [m] (zs/indexed-zset+ m grouped-by-result))))))))))

(defn init-atoms! []
  )

(defn graph->zsxf [graph]

  (join-one-level-2 :a (atom nil) :b (atom nil)))

(defn incrementalize! [query]
  (let [where-clauses (get query :where)]
    (graph->zsxf
      (parser/where-clauses-to-graph where-clauses))))


(comment
  (macroexpand
    '(join-one-level :player/team :team-name "Team A"))

  (join-one-level-2 :player/team :team-name "Team A")

  (incrementalize!
    '[[?team-eid :team/name ?team-name]
      [?player-eid :player/team ?team-eid]]))

(defmacro where [node]
  )

(defmacro graph->code [graph]
  (transduce
    ;TODO WIP
    (map (fn [element] element))
    conj
    '()
    graph))

(comment
  (parser/where-clauses-to-graph
    '[[?team-eid :team/name ?team-name]
      [?player-eid :player/team ?team-eid]
      [?a :b ?c]]

    )

  (parser/where-clauses-to-graph
    '[[?user :user/name "Eve"]
      [?login :login/user ?user]]

    )

  ;=>
  ; does it matter if player or team is first?
  ;  - likely no
  ; create atoms on the fly?
  ; - likely yes
  ;
  {:graph        #{[?a :b ?c] [?player-eid :player/team [?team-eid :team/name ?team-name]]},
   :used-clauses #{[?player-eid :player/team ?team-eid]}}
  )
