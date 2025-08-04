(ns org.saberstack.clock.monotonic
  "Monotonic clock that guarantees strictly increasing timestamps.")

(def ^:private generation-clock
  "Atom holding [timestamp generation] state for a monotonic clock."
  (atom (vector 0 0)))

(defn- increment-clock
  "Ensures monotonic progression given [prev-t generation] and new timestamp t.
   Returns [new-timestamp new-generation]"
  [[^long prev-t ^long generation] ^long next-t]
  (if (<= next-t prev-t)
    ;this case happens if the system clock goes backwards
    (vector (unchecked-inc prev-t) (unchecked-inc generation))
    ;this is the more typical case: system clock goes forward
    (vector next-t (unchecked-inc generation))))

(defn- now-impl
  "Internal function that atomically updates generation-clock with current time."
  []
  (swap! generation-clock increment-clock (* 1000000 (System/currentTimeMillis))))

(defn now
  "Returns monotonic timestamp."
  []
  (nth (now-impl) 0))

(defn now+generation
  "Returns [timestamp generation] vector with both monotonic time and counter."
  []
  (now-impl))
