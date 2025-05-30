^{:nextjournal.clerk/visibility {:code :hide}}
(ns datascript-demo
  (:require [nextjournal.clerk :as clerk]
            [datascript.core :as d]
            [clojure.string :as str]))

;; # Datascript Query Engine Comparison Demo

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

^{:nextjournal.clerk/visibility {:code :hide :result :show}}
(clerk/html
 [:div {:style {:padding "20px" :border "1px solid #ccc" :margin "10px 0"}}
  [:h3 "Time Range Selection"]
  
  ;; Include noUiSlider CSS and JS from CDN
  [:link {:rel "stylesheet" 
          :href "https://cdnjs.cloudflare.com/ajax/libs/noUiSlider/15.7.1/nouislider.min.css"}]
  [:script {:src "https://cdnjs.cloudflare.com/ajax/libs/noUiSlider/15.7.1/nouislider.min.js"}]
  
  ;; Slider container
  [:div {:id "time-range-slider" 
         :style {:margin "30px 0" :height "30px"}}]
  
  ;; Display current values
  [:div {:style {:display "flex" :justify-content "space-between" :margin "20px 0" :font-size "16px"}}
   [:span [:strong "Start Year: "] [:span {:id "start-display"} "1900"]]
   [:span [:strong "End Year: "] [:span {:id "end-display"} "2000"]]]
  
  [:button {:onclick "runDemo()" 
            :style {:padding "12px 24px" 
                    :margin "10px 0" 
                    :background "#007acc" 
                    :color "white" 
                    :border "none" 
                    :border-radius "4px"
                    :cursor "pointer"
                    :font-size "16px"}}
   "Run Query"]
  
  ;; Custom styling for the slider
  [:style "
   #time-range-slider {
     background: #ddd;
     border-radius: 5px;
     height: 10px !important;
   }
   
   #time-range-slider .noUi-connect {
     background: #007acc;
   }
   
   #time-range-slider .noUi-handle {
     background: #007acc;
     border: 2px solid white;
     box-shadow: 0 2px 4px rgba(0,0,0,0.2);
     border-radius: 50%;
     cursor: pointer;
     width: 20px;
     height: 20px;
   }
   
   #time-range-slider .noUi-handle:before,
   #time-range-slider .noUi-handle:after {
     display: none;
   }
   
   #time-range-slider .noUi-tooltip {
     background: #333;
     color: white;
     border: none;
     border-radius: 3px;
     padding: 4px 8px;
     font-size: 12px;
   }
   
   button:hover {
     background: #005a99 !important;
   }"]
  
  [:script "
   // Global variables to store current range
   window.currentStartYear = 1900;
   window.currentEndYear = 2000;
   
   function initializeSlider() {
     const slider = document.getElementById('time-range-slider');
     
     // Check if noUiSlider is loaded and slider exists
     if (typeof noUiSlider === 'undefined' || !slider) {
       console.log('Waiting for noUiSlider...');
       setTimeout(initializeSlider, 200);
       return;
     }
     
     // Destroy existing slider if it exists
     if (slider.noUiSlider) {
       slider.noUiSlider.destroy();
     }
     
     try {
       // Create the slider
       noUiSlider.create(slider, {
         start: [1900, 2000],
         connect: true,
         range: {
           'min': 1871,
           'max': 2023
         },
         step: 1,
         format: {
           to: function(value) {
             return Math.round(value);
           },
           from: function(value) {
             return Number(value);
           }
         },
         tooltips: [
           {to: function(value) { return Math.round(value); }},
           {to: function(value) { return Math.round(value); }}
         ]
       });
       
       // Update displays when slider changes
       slider.noUiSlider.on('update', function(values, handle) {
         const startYear = Math.round(values[0]);
         const endYear = Math.round(values[1]);
         
         window.currentStartYear = startYear;
         window.currentEndYear = endYear;
         
         const startEl = document.getElementById('start-display');
         const endEl = document.getElementById('end-display');
         
         if (startEl) startEl.textContent = startYear;
         if (endEl) endEl.textContent = endYear;
       });
       
       console.log('noUiSlider initialized successfully');
       
     } catch (error) {
       console.error('Error initializing noUiSlider:', error);
     }
   }
   
   function runDemo() {
     // Call back to Clojure function with current values
     const startYear = window.currentStartYear || 1900;
     const endYear = window.currentEndYear || 2000;
     
     console.log('Running demo with range:', startYear, 'to', endYear);
     
     // This calls back to your Clojure function
     if (typeof window.clerk_eval === 'function') {
       window.clerk_eval('(run-comparison-with-display ' + startYear + ' ' + endYear + ')');
     } else {
       console.log('clerk_eval not available, would call: (run-comparison-with-display ' + startYear + ' ' + endYear + ')');
     }
   }
   
   // Initialize when the script loads
   setTimeout(initializeSlider, 500);
   
   // Also try to initialize on load events
   if (document.readyState === 'loading') {
     document.addEventListener('DOMContentLoaded', initializeSlider);
   }
   window.addEventListener('load', initializeSlider);
   "]])

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
