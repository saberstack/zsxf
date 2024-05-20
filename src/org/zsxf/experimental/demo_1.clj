(ns org.zsxf.experimental.demo-1)

(defonce *state {:refreshing? false
                 :data {}})
(defn refresh-data!
  "Auto refresh data from database.
  Only for demo purpose."
  []
  ;TODO Implement
  (println "Refreshing data from database..."))

(defn init []
  (when-not (get @*state :refreshing?)
    (refresh-data!)
    (swap! *state assoc :refreshing? true)))
