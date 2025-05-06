(ns org.zsxf.alpha.persistence
  (:require [clj-memory-meter.core :as mm]
            [datascript.core :as d]
            [org.zsxf.datom :as d2]
            [taoensso.nippy :as nippy])
  (:import (datascript.db Datom)
           (java.io DataOutputStream)
           (org.zsxf.type.datom Datom2)))


(defn write-clj-type [^DataOutputStream data-output x]
  (let [^bytes ba (nippy/freeze x)]
    (.write ^DataOutputStream data-output ba 0 (alength ba))))


(defn nippy-freeze-datom
  [^DataOutputStream data-output ^Datom datom]
  (.writeInt data-output (.-e datom))
  (write-clj-type data-output (.-a datom))
  (write-clj-type data-output (.-v datom)))

(defn nippy-freeze-datom2
  [^DataOutputStream data-output ^Datom2 datom2]
  (write-clj-type data-output (.-datom datom2))
  (write-clj-type data-output (.-meta datom2)))

(nippy/extend-freeze Datom :datascript.db/Datom
  [x data-output]
  (nippy-freeze-datom data-output x))

(nippy/extend-freeze Datom2 :org.zsxf.type/Datom2
  [x data-output]
  (nippy-freeze-datom2 data-output x))

(comment

  (count (nippy/freeze (d/datom 1 :a "v")))

  (mm/measure (d/datom 1 :a "v"))
  (mm/measure (nippy/freeze (d/datom 1 :a "v")))

  (mm/measure (nippy/freeze (d2/datom2 (d/datom 1 :a "v")))))
