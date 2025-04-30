(ns org.zsxf.datalog.fn
  #?(:cljs (:require [goog.array :as garray]))
  #?(:clj (:import (clojure.lang Numbers Util))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Code below mostly from Datascript, less some macros
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(declare value-compare)

#?(:clj
   (defn class-name
     {:inline
      (fn [x]
        `(let [^Object x# ~x]
           (if (nil? x#) x# (.getName (. x# (getClass))))))}
     ^String [^Object x] (if (nil? x) x (.getName (. x (getClass))))))

(defn class-compare
  ^long [x y]
  #?(:clj  (long (compare (class-name x) (class-name y)))
     :cljs (garray/defaultCompare (type->str (type x)) (type->str (type y)))))

(defn ihash
  {:inline (fn [x] `(. Util (hasheq ~x)))}
  ^long [x]
  #?(:clj  (. Util (hasheq x))
     :cljs (hash x)))

(defn class-identical?
  #?(:clj {:inline (fn [x y] `(identical? (class ~x) (class ~y)))})
  [x y]
  #?(:clj  (identical? (class x) (class y))
     :cljs (identical? (type x) (type y))))

#?(:cljs
   (defmacro int-compare [x y]
     `(- ~x ~y)))
#?(:clj
   (defmacro int-compare [x y]
     `(long (Integer/compare ~x ~y))))

(defn seq-compare [xs ys]
  (let [cx (count xs)
        cy (count ys)]
    (cond
      (< cx cy)
      -1
      (> cx cy)
      1
      :else
      (loop [xs xs
             ys ys]
        (if (empty? xs)
          0
          (let [x (first xs)
                y (first ys)]
            (cond
              (and (nil? x) (nil? y))
              (recur (next xs) (next ys))
              (nil? x)
              -1
              (nil? y)
              1
              :else
              (let [v (value-compare x y)]
                (if (= v 0)
                  (recur (next xs) (next ys))
                  v)))))))))

(defn value-compare [x y]
  (try
    (cond
      (= x y) 0
      (and (sequential? x) (sequential? y)) (seq-compare x y)
      #?@(:clj [(instance? Number x) (Numbers/compare x y)])
      #?@(:clj  [(instance? Comparable x) (.compareTo ^Comparable x y)]
          :cljs [(satisfies? IComparable x) (-compare x y)])
      (not (class-identical? x y)) (class-compare x y)
      #?@(:cljs [(or (number? x) (string? x) (array? x) (true? x) (false? x)) (garray/defaultCompare x y)])
      :else (int-compare (ihash x) (ihash y)))
    (catch #?(:clj ClassCastException :cljs js/Error) e
      (if (not (class-identical? x y))
        (class-compare x y)
        (throw e)))))

(defn less
  ([x] true)
  ([x y] (neg? (value-compare x y)))
  ([x y & more]
   (if (less x y)
     (if (next more)
       (recur y (first more) (next more))
       (less y (first more)))
     false)))

(defn greater
  ([x] true)
  ([x y] (pos? (value-compare x y)))
  ([x y & more]
   (if (greater x y)
     (if (next more)
       (recur y (first more) (next more))
       (greater y (first more)))
     false)))

(defn less-equal
  ([x] true)
  ([x y] (not (pos? (value-compare x y))))
  ([x y & more]
   (if (less-equal x y)
     (if (next more)
       (recur y (first more) (next more))
       (less-equal y (first more)))
     false)))

(defn greater-equal
  ([x] true)
  ([x y] (not (neg? (value-compare x y))))
  ([x y & more]
   (if (greater-equal x y)
     (if (next more)
       (recur y (first more) (next more))
       (greater-equal y (first more)))
     false)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; End section
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def query-fns
  {'=                      `=
   '==                     `==
   'not=                   `not=
   '!=                     `not=
   '<                      `less
   '>                      `greater
   '<=                     `less-equal
   '>=                     `greater-equal
   'clojure.core/distinct? `distinct?
   'distinct?              `distinct?})
