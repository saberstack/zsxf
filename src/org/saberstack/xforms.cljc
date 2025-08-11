(ns org.saberstack.xforms
  "Extra Clojure transducers, including
   - Conditional transducers for selective data transformation only when specified predicates are satisfied.

   Alpha, subject to change.")

(defn map-when
  "Returns a transducer that conditionally transforms items.

  For each input item, applies the test predicate. If the test returns truthy,
  applies the transformation function f to the item. Otherwise, passes the item
  through unchanged.

  Args:
    test - A predicate function that takes an item and returns truthy/falsy
    f    - A transformation function applied to items that pass the test

  Returns:
    A transducer that can be used with transduce, into, sequence, etc.

  Example:
    (into [] (map-when odd? inc) [1 2 3 4 5])
    ;; => [2 2 4 4 6]
    ;; Only odd numbers (1, 3, 5) are incremented"
  ([test f]
   (fn [rf]
     (fn
       ([] (rf))
       ([result] (rf result))
       ([result input]
        (rf result
          (if (test input)
            (f input)
            input)))))))

(defn ^:private preserving-reduced
  [rf]
  #(let [ret (rf %1 %2)]
     (if (reduced? ret)
       (reduced ret)
       ret)))

(defn cat-when
  "Conditionally concatenates (flattens) collections based on a predicate test.

  Returns a transducer that applies concatenation semantics only when the test
  predicate returns truthy for the input. When the test fails, the input is
  passed through unchanged as a single item rather than being concatenated.

  This is equivalent to applying `cat` (mapcat identity) selectively - collections
  that pass the test are flattened into their constituent elements, while those
  that fail are preserved as single items.

  Args:
    test - A predicate function that takes an input and returns truthy/falsy

  Returns:
    A transducer that can be used with transduce, into, sequence, etc.

  Behavior:
    - When (test input) is truthy: behaves like `cat`, flattening the input
    - When (test input) is falsy: passes input through as a single item
    - Properly handles reduced values to allow early termination

  Examples:
    ;; Only flatten vectors, pass other collections as single items
    (into [] (cat-when vector?) [[1 2] {:a 1} #{5 6}])
    ;; => [1 2 {:a 1} #{6 5}]

    ;; Only flatten non-empty collections
    (into [] (cat-when seq) [[] [1 2] [] [3 4 5]])
    ;; => [[] 1 2 [] 3 4 5]

    ;; Flatten collections longer than 2 elements
    (into [] (cat-when #(> (count %) 2)) [[1] [1 2] [1 2 3 4]])
    ;; => [[1] [1 2] 1 2 3 4]"

  [test]
  (fn [rf]
    (let [rrf (preserving-reduced rf)]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result input]
         (if (test input)
           (reduce rrf result input)
           (rf result input)))))))
