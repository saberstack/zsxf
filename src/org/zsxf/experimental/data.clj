(ns org.zsxf.experimental.data
  (:require [org.zsxf.zset :as zs]))

(defn data1 []
  (zs/->zset
    [{:team/id 1 :team/name "T1"}
     {:team/id 2 :team/name "T2"}
     {:player/name "Alice" :player/team 1}
     {:player/name "Bob" :player/team 2}
     {:player/name "Chris" :player/team 2}]))

(defn data2 []
  (zs/->zset
    [{:team/id 3 :team/name "T3"}
     {:team/id 4 :team/name "T4"}
     {:player/name "A" :player/team 3}
     {:player/name "B" :player/team 3}
     {:player/name "C" :player/team 4}]))
