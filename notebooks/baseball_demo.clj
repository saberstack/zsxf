^{:nextjournal.clerk/visibility {:code :hide}}
(ns baseball-demo
  (:require [nextjournal.clerk :as clerk]
            [datascript.core :as d]
            [clojure.string :as str]
            [nextjournal.clerk.experimental :as cx]))

;; # Datascript Query Engine Comparison Demo

#_(cx/slider {:min 1870 :max 2023 :step 1} @state )
^{::clerk/sync true ::clerk/viewer (partial cx/slider {:min 1870 :max 2023 :step 1})}
(def state (atom 2000))

^{:nextjournal.clerk/visibility {:code :hide}}
(do
  ;; Atom to hold our base database
  (defonce db-atom (atom nil))
  
  ;; Atom to accumulate performance measurements for plotting
  (defonce performance-data (atom []))
  
  ;; Atom to store latest results for display
  (defonce latest-result (atom nil))
  
  ;; Initialize your base database (you'll implement this)
  (defn init-database! []
    ;; TODO: Your code to create the initial Datascript database
    ;; This should load your base schema and any foundational data
    (reset! db-atom (d/create-conn {}))) ; placeholder schema
  
  ;; Your function to update database based on time slice
  (defn update-db-for-timeframe! [start-year end-year]
    ;; TODO: Your code to transact/retract datoms for the time window
    ;; This modifies @db-atom to reflect only data from start-year to end-year
    (println "Updating DB for range:" start-year "to" end-year))
  
  ;; Your query engines (you'll implement these)
  (defn query-engine-1 [db query]
    ;; TODO: First query engine implementation
    ;; Returns [result execution-time-ms]
    [[] (+ 10 (rand-int 50))]) ; placeholder
  
  (defn query-engine-2 [db query]
    ;; TODO: Second query engine implementation  
    ;; Returns [result execution-time-ms]
    [[] (+ 20 (rand-int 80))]) ; placeholder
  
  ;; Your main demo query - modify this to be interesting for your data
  (def demo-query 
    '[:find ?entity ?attribute ?value
      :where [?entity ?attribute ?value]])
  
  ;; Function that gets called when user clicks "Run Query"
  (defn run-comparison-with-display [start-year end-year]
    (let [start-time (System/currentTimeMillis)]
      
      ;; Update database for the selected time range
      (update-db-for-timeframe! start-year end-year)
      
      ;; Get current database value
      (let [db @db-atom
            db-size (if db (count (d/datoms db :eavt)) 0)]
        
        ;; Run query with both engines
        (let [[result1 time1] (query-engine-1 db demo-query)
              [result2 time2] (query-engine-2 db demo-query)]
          
          ;; Store performance data for plotting
          (swap! performance-data conj
                 {:timeframe (str start-year "-" end-year)
                  :start-year start-year
                  :end-year end-year
                  :db-size db-size
                  :engine1-time time1
                  :engine2-time time2
                  :timestamp (System/currentTimeMillis)})
          
          ;; Store result for display
          (let [result {:query-result result1
                        :database-size db-size
                        :engine1-time time1
                        :engine2-time time2
                        :timeframe (str start-year "-" end-year)}]
            (reset! latest-result result)
            result))))))

;; ## Interactive Controls

^{:nextjournal.clerk/visibility {:code :hide}}
(defonce !start-year (atom 1900))

^{:nextjournal.clerk/visibility {:code :hide}}
(defonce !end-year (atom 2000))

^{:nextjournal.clerk/visibility {:code :hide :result :show}}
(clerk/with-viewer
  {:transform-fn (comp clerk/mark-presented (clerk/update-val (fn [_] [@!start-year @!end-year])))
   :render-fn 
   '(fn [[start-year end-year] _]
      [:div {:style {:padding "20px" :border "1px solid #ccc" :margin "10px 0"}}
       [:h3 "Time Range Selection"]
       [:div {:style {:display "flex" :gap "15px" :align-items "center" :margin "20px 0"}}
        [:label "Start Year:"]
        [:input {:type "number" 
                 :value start-year
                 :min 1871 
                 :max 2023
                 :style {:width "80px" :padding "8px" :border "1px solid #ccc"}
                 :on-change (fn [e] 
                             (let [val (js/parseInt (.. e -target -value))]
                               (nextjournal.clerk.render/clerk-eval 
                                 `(reset! ~'!start-year ~val))))}]
        [:span "to"]
        [:input {:type "number" 
                 :value end-year
                 :min 1871 
                 :max 2023
                 :style {:width "80px" :padding "8px" :border "1px solid #ccc"}
                 :on-change (fn [e] 
                             (let [val (js/parseInt (.. e -target -value))]
                               (nextjournal.clerk.render/clerk-eval 
                                 `(reset! ~'!end-year ~val))))}]]
       [:button {:style {:padding "12px 24px" 
                         :margin "10px 0" 
                         :background "#007acc" 
                         :color "white" 
                         :border "none" 
                         :border-radius "4px"
                         :cursor "pointer"}
                 :on-click (fn [_]
                            (nextjournal.clerk.render/clerk-eval 
                              `(run-comparison-with-display @~'!start-year @~'!end-year)))}
        "Run Query"]])}
  {})

^{:nextjournal.clerk/visibility {:code :hide}}
(defn run-query-manually 
  "Helper function to run the query with current atom values"
  []
  (run-comparison-with-display @!start-year @!end-year))

;; ## Current Results

^{:nextjournal.clerk/visibility {:code :hide :result :show}}
(when @latest-result
  (let [{:keys [query-result database-size engine1-time engine2-time timeframe]} @latest-result]
    (clerk/html
     [:div {:style {:margin "20px 0"}}
      [:h3 (str "Query Results for " timeframe)]
      [:p (str "Database size: " database-size " datoms")]
      [:p (str "Engine 1 time: " engine1-time "ms")]
      [:p (str "Engine 2 time: " engine2-time "ms")]
      
      ;; Simple table rendering - you can enhance this
      [:table {:style {:border-collapse "collapse" :width "100%"}}
       [:thead
        [:tr {:style {:background "#f0f0f0"}}
         [:th {:style {:border "1px solid #ccc" :padding "8px"}} "Entity"]
         [:th {:style {:border "1px solid #ccc" :padding "8px"}} "Attribute"] 
         [:th {:style {:border "1px solid #ccc" :padding "8px"}} "Value"]]]
       [:tbody
        (for [[entity attr value] (take 20 query-result)] ; Show first 20 rows
          [:tr
           [:td {:style {:border "1px solid #ccc" :padding "8px"}} (str entity)]
           [:td {:style {:border "1px solid #ccc" :padding "8px"}} (str attr)]
           [:td {:style {:border "1px solid #ccc" :padding "8px"}} (str value)]])]]])))

;; ## Performance Comparison

^{:nextjournal.clerk/visibility {:code :hide :result :show}}
(when (seq @performance-data)
  (clerk/vl
    {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
     :title "Query Engine Performance Comparison"
     :width 600
     :height 400
     :data {:values @performance-data}
     :layer 
     [{:mark {:type "point" :filled true :size 100 :color "#1f77b4"}
       :encoding {:x {:field "db-size" :type "quantitative" :title "Database Size (datoms)"}
                  :y {:field "engine1-time" :type "quantitative" :title "Execution Time (ms)"}
                  :tooltip [{:field "timeframe" :title "Time Range"}
                           {:field "db-size" :title "DB Size"}
                           {:field "engine1-time" :title "Engine 1 Time (ms)"}]}}
      
      {:mark {:type "point" :filled true :size 100 :color "#ff7f0e"}
       :encoding {:x {:field "db-size" :type "quantitative"}
                  :y {:field "engine2-time" :type "quantitative"}
                  :tooltip [{:field "timeframe" :title "Time Range"}
                           {:field "db-size" :title "DB Size"}
                           {:field "engine2-time" :title "Engine 2 Time (ms)"}]}}]
     
     :resolve {:scale {:color "independent"}}
     :config {:legend {:orient "bottom"}}}))

^{:nextjournal.clerk/visibility {:code :hide}}
(comment
  ;; Initialize the database when the notebook loads
  (init-database!))
