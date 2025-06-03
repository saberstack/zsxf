(ns baseball-demo
  {:nextjournal.clerk/visibility {:code :hide :result :hide}}
                                        ;^{:nextjournal.clerk/visibility {:code :hide }}
  (:require [nextjournal.clerk :as clerk]
            [datascript.core :as d]
            [org.zsxf.test-data.baseball-etl :as etl]

            [nextjournal.clerk.viewer :as viewer]))

(def max-year 2022)
(def min-year 1871)
(def init-range {:first 1980 :second 1985})

;; # Datascript Query Engine Comparison Demo

(defn render-input-pair
  ([] (render-input-pair {}))
  ([opts] (list 'partial
                '(fn [{:as opts :keys [placeholder-1 placeholder-2] :or {placeholder-1 "min" placeholder-2 "max"}} !state]
                   [:div
                    [:input {:type :text
                              :id "first-ipt"
                              :placeholder placeholder-1
                              :value (:first @!state)
                              :class "mx-[40px] py-3 placeholder-blueGray-300 text-blueGray-600 relative bg-white bg-white rounded text-sm border border-blueGray-300 outline-none focus:outline-none focus:ring w-[25%]"
                              :on-change #(swap! !state assoc :first (js/parseInt (.. % -target -value)))}]
                    [:input {:type :text
                              :id "second-ipt"
                              :placeholder placeholder-2
                              :value (:second @!state)
                              :class "mx-[40px] py-3 placeholder-blueGray-300 text-blueGray-600 relative bg-white bg-white rounded text-sm border border-blueGray-300 outline-none focus:outline-none focus:ring w-[25%]"
                              :on-change #(swap! !state assoc :second (js/parseInt (.. % -target -value )))}]])
                opts)))

(defn input-pair
  ([!state] (input-pair {} !state))
  ([opts !state] (viewer/with-viewer (assoc viewer/viewer-eval-viewer :render-fn (render-input-pair opts)) !state)))

;; ### Year slice

^{::clerk/sync true ::clerk/viewer input-pair ::clerk/visibility {:result :show}}
(defonce year-ipt (atom init-range))

(defonce db-atom (atom (etl/fresh-conn)))

(defonce performance-data (atom []))

(defonce latest-result (atom nil))

(defn update-db-for-timeframe! [start-year end-year]
  (etl/populate-datascript-db @db-atom {:min-year start-year :max-year end-year}))

(def demo-query
  '[:find  ?player-name ?team-name
    :where
    [?p :player/name ?player-name]
    [?season :tenure/player ?p]
    [?season :tenure/team ?t]
    [?t :team/name ?team-name]])

(defn query-engine-1 [db query]
  ;; Returns [result execution-time-ms]
  [(d/q query db) (+ 10 (rand-int 50))]) ; placeholder

(defn query-engine-2 [db query]
  ;; Returns [result execution-time-ms]
  [[] (+ 20 (rand-int 80))]) ; placeholder


(defn run-comparison-with-display [start-year end-year]
  (when (and (>= start-year min-year)
             (<= end-year max-year)
             (> end-year start-year))
    (let [start-time (System/currentTimeMillis)]

      (update-db-for-timeframe! start-year end-year)

      (let [db @@db-atom
            db-size (if db (count (d/datoms db :eavt)) 0)]

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

(apply run-comparison-with-display ((juxt :first :second ) @year-ipt))

;; ## Current Results
^{::clerk/visibility {:result :show} ::clerk/no-cache true}
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
        (for [[entity attr value] (take 5 query-result)] ; Show first 20 rows
          [:tr
           [:td {:style {:border "1px solid #ccc" :padding "8px"}} (str entity)]
           [:td {:style {:border "1px solid #ccc" :padding "8px"}} (str attr)]
           [:td {:style {:border "1px solid #ccc" :padding "8px"}} (str value)]])]]])))

;; ## Performance Comparison
^{::clerk/visibility {:result :show } ::clerk/no-cache true}
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


(defn reset-to-initial []
  (reset! db-atom (etl/fresh-conn))
  (reset! year-ipt init-range))

(comment
  (reset-to-initial)
  (reset! db-atom (etl/fresh-conn))

  )
