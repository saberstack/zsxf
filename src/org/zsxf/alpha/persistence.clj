(ns org.zsxf.alpha.persistence
  (:require [clj-memory-meter.core :as mm]
            [datascript.core :as d]
            [org.zsxf.datom :as d2]
            [taoensso.nippy :as nippy])
  (:import (datascript.db Datom)
           (java.io DataInputStream DataOutputStream)
           (org.zsxf.type.datascript.datom2 Datom2)))

(defn nippy-freeze-datom
  [^DataOutputStream data-output ^Datom datom]
  (nippy/freeze-to-out! data-output [(.-e datom) (.-a datom) (.-v datom)]))

(defn nippy-freeze-datom2
  [^DataOutputStream data-output ^Datom2 datom2]
  (nippy/freeze-to-out! data-output [(.-datom datom2) (.-meta datom2)]))

(defn nippy-thaw-datom
  [^DataInputStream data-input]
  (let [[e a v] (nippy/thaw-from-in! data-input)]
    (d/datom e a v)))

(defn nippy-thaw-datom2
  [^DataInputStream data-input]
  (let [[datom meta] (nippy/thaw-from-in! data-input)]
    (d2/datom2 datom meta)))

(nippy/extend-freeze Datom :datascript.db/Datom
  [x data-output]
  (nippy-freeze-datom data-output x))

(nippy/extend-thaw :datascript.db/Datom
  [data-input]
  (nippy-thaw-datom data-input))

(nippy/extend-freeze Datom2 :org.zsxf.type/Datom2
  [x data-output]
  (nippy-freeze-datom2 data-output x))

(nippy/extend-thaw :org.zsxf.type/Datom2
  [data-input]
  (nippy-thaw-datom2 data-input))

(comment

  (count (nippy/freeze (d/datom 1 :a "v")))

  (mm/measure (d/datom 1 :a "v"))
  (mm/measure (nippy/freeze (d/datom 1 :a "v")))

  (mm/measure (nippy/freeze (d2/datom2 (d/datom 1 :a "v")))))

#_(defn write-clj-type [^DataOutputStream data-output x]
    (let [^bytes ba (nippy/freeze x)]
      (.write ^DataOutputStream data-output ba 0 (alength ba))))
