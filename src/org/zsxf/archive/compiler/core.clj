(ns org.zsxf.archive.compiler.core
  (:require [datascript.core :as d]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; STEP 1: Obtain a stream of database changes (transaction data) from DataScript
;; TODO
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; STEP 2: Transform Datalog queries into intermediate representation which acts as the query plan
;; We don't need to get fancy here because S-expressions are a great IR!

(comment
  ;; from a Datalog query ...
  (def example-datalog-query-1
    '[:find ?exercise-set-eids                              ;;eids are integers
      :in $ ?workout-uuid ?person-eid
      :where
      [?person-eid :person/workouts ?workout-eid]
      [?workout-eid :workout/uuid ?workout-uuid]
      [?exercise-set-eids :exercise-set/workout ?workout-eid]])

  ;; ... to a graph! Datalog queries form an implicit graph that we can "unroll"
  [?exercise-set-eids :exercise-set/workout #_?workout-eid
   [#_?workout-eid [?person-eid :person/workouts ?workout-eid] :workout/uuid ?workout-uuid]])


(comment
  [?exercise-set-eids :exercise-set/workout
   [[?person-eid :person/workouts ?workout-eid] :workout/uuid ?workout-uuid]]

  [?person-eid :person/workouts
   [[?exercise-set-eids :exercise-set/workout ?workout-eid] :workout/uuid ?workout-uuid]])

(comment
  (def teams-and-players-query-2
    '[:find ?player-name
      :in ?team-name
      :where
      [?team-eid :team/name ?team-name]
      [_ :player/team ?team-eid]
      [_ :player/name ?player-name]]))

(comment
  [;?player-name is what we are looking for

   [_ :player/name ?player-name]

   [?player-eid :player/team
    ;... "points" to ?team-eid
    ; ?team-name gets resolved from context (aka the query :in clause)
    [?team-eid :team/name "A"]]

   ])

; Explore change cases:

;; :team/name changes name from "A" to "B"
;;; Expected: the result set becomes #{}

;; :player/team changes (player goes to another team)
;;; Expected: the player disappears from the result set, #{["A"]} is now #{}


;; Example start
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def schema-teams-players-1
  {:team/name   {:db/cardinality :db.cardinality/one
                 :db/unique      :db.unique/identity}
   :player/team {:db/cardinality :db.cardinality/one
                 :db/valueType   :db.type/ref}
   :player/name {:db/cardinality :db.cardinality/one}})

(defonce *conn-1 (atom nil))

(defonce *transactions-1 (atom []))

(defn clear-state! []
  (reset! *conn-1 nil)
  (reset! *transactions-1 []))

(defn init-datascript! []
  (clear-state!)                                            ;clear REPL state
  (let [conn (d/create-conn schema-teams-players-1)
        _    (reset! *conn-1 conn)
        tx   (d/transact! conn
               [{:db/id     -42
                 :team/name "A"}
                {:player/name "Alice"
                 :player/team -42}])
        _    (swap! *transactions-1 conj tx)]
    (d/q '[:find ?team-name
           :where
           [_ :team/name ?team-name]]
      @conn)))

;;Usage
(comment
  (init-datascript!)
  @@*conn-1
  @*transactions-1
  (mapv :tx-data @*transactions-1)
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Question:
;; How do we update the results incrementally if data downstream in the graph causes the results to change?
;; Does Calcite help with this problem?

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; STEP 3: Replace each node in the IR graph with a ZSXF transducer
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; This should be _relatively_ straightforward once the query plan is done



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Sample DataScript schema (mostly Datomic compatible, small changes needed)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def schema
  {
   ;person
   ;-----------------------------------------------------------
   :person/uuid
   ;uuid
   {:db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

   :person/workouts
   {:db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}

   :person/weekly-goal
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}

   :person/weekly-goals
   {:db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}

   ;time
   ;-----------------------------------------------------------
   :time/timestamp
   {:db/cardinality :db.cardinality/one
    :db/index       true}

   :time/tz
   {:db/cardinality :db.cardinality/one
    :db/index       true}

   ;exercise-set
   ;-----------------------------------------------------------
   :exercise-set/uuid
   ;uuid
   {:db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

   :exercise-set/person
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}

   :exercise-set/exercise
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}

   :exercise-set/exercise-data
   ;examples: rep-count, weight, intensity-score
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref
    :db/isComponent true}

   :exercise-set/rep-count
   {:db/cardinality :db.cardinality/one}

   :exercise-set/weight
   {:db/cardinality :db.cardinality/one}

   :exercise-set/rep-accelerations
   {:db/cardinality :db.cardinality/one}
   ;vector like [0.33 0.25 0.15]
   ;each element is one rep acceleration average expressed in m/s^2

   :exercise-set/all-accelerations
   {:db/cardinality :db.cardinality/one}
   ;vector like

   :exercise-set/start-time
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref
    :db/index       true
    :db/isComponent true}

   :exercise-set/end-time
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref
    :db/index       true
    :db/isComponent true}

   :exercise-set/done-scheduled-exercise
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}

   :exercise-set/workout
   ;exercise-set can be a part of a workout (not required)
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}


   ;exercise (a few)
   ;-----------------------------------------------------------
   :exercise/uuid
   ;uuid
   {:db.cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

   :exercise/name
   {:db/cardinality :db.cardinality/one
    :db/unique      :db.unique/value}                       ;aka SQL unique index

   ;goals
   ;;-----------------------------------------------------------
   ;:goal/uuid
   ;;uuid
   ;{:db/cardinality :db.cardinality/one
   ; :db/unique      :db.unique/identity}
   ;
   ;:goal/person
   ;{:db/cardinality :db.cardinality/one
   ; :db/valueType   :db.type/ref}
   ;
   ;:goal/name
   ;{:db/cardinality :db.cardinality/one}
   ;
   ;:goal/total-reps
   ;{:db/cardinality :db.cardinality/one}
   ;
   ;:goal/done-reps
   ;{:db/cardinality :db.cardinality/one}
   ;
   ;:goal/exercise
   ;{:db/cardinality :db.cardinality/one
   ; :db/valueType   :db.type/ref}

   ;scheduled exercises
   ;-----------------------------------------------------------
   :scheduled-exercise/uuid
   ;uuid
   {:db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}

   :scheduled-exercise/person
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}

   :scheduled-exercise/exercise
   ;one exercise
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}

   :scheduled-exercise/day-of-week
   ;string
   {:db/cardinality :db.cardinality/one}

   :scheduled-exercise/rep-set-scheme-v1
   ;vector like
   #_[6 6 [:drop-set [6 8 10]]]
   {:db/cardinality :db.cardinality/one}

   :scheduled-exercise/sort-id
   {:db/cardinality :db.cardinality/one}

   ;workout plan
   ;-----------------------------------------------------------
   :workout-plan/uuid
   ;uuid
   {:db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}


   :workout-plan/created-time
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref
    :db/index       true
    :db/isComponent true}


   :workout-plan/title
   ;string
   {:db/cardinality :db.cardinality/one}


   :workout-plan/person
   ;the person
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}


   :workout-plan/rest-seconds
   {:db/cardinality :db.cardinality/one}


   :workout-plan/exercise|rep-set-scheme|sort-id
   {:db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}
   ;{:ref/exercise {...} :vec/rep-set-scheme [6 6 6] :long/sort-id 42}


   ;workout
   ;-----------------------------------------------------------
   :workout/uuid
   ;uuid
   {:db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity}


   :workout/person
   ;the person
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}

   :workout/workout-plan
   ;a workout-plan
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}

   :workout/start-time
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref
    :db/index       true
    :db/isComponent true}

   :workout/end-time
   ;ok
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref
    :db/index       true
    :db/isComponent true}

   ;refs
   ;-----------------------------------------------------------
   :ref/exercise
   ;one exercises
   {:db/cardinality :db.cardinality/one
    :db/valueType   :db.type/ref}

   ;numbers
   ;-----------------------------------------------------------
   :long/sort-id
   {:db/cardinality :db.cardinality/one}

   :long/sort-id-2
   {:db/cardinality :db.cardinality/one}

   ;etc
   ;-----------------------------------------------------------
   :vec/rep-set-scheme
   {:db/cardinality :db.cardinality/one}})
