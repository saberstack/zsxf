(ns baseball-demo
  {:nextjournal.clerk/visibility {:code :hide :result :hide}}
  (:require [nextjournal.clerk :as clerk]
            [datascript.core :as d]
            [org.zsxf.test-data.baseball-etl :as etl]
            [org.zsxf.util :as util]
            [nextjournal.clerk.viewer :as viewer]
            [org.zsxf.input.datascript :as ds]
            [org.zsxf.datalog.compiler :refer [static-compile]]
            [taoensso.timbre :as timbre]
            [org.zsxf.query :as q]))

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

(defonce zquery (q/create-query
                (static-compile '[:find  ?p ?player-name (sum ?home-runs)
                                  :where
                                  [?p :player/name ?player-name]
                                  [?season :season/player ?p]
                                  [?season :season/year ?year]
                                  [?season :season/home-runs ?home-runs]])))

(defn add-zsxf-query [conn]
  (when (seq (ds/conn-listeners conn))
      (throw (Exception. "There is already a listener on this connection; there should only be one." {})))
    (ds/init-query-with-conn zquery conn :tx-time-f #(swap! zsxf-times conj %))
    conn)

(def new-db (comp add-zsxf-query etl/fresh-conn))

;; ### Year slice

^{::clerk/sync true ::clerk/viewer input-pair ::clerk/visibility {:result :show}}
(defonce year-ipt (atom init-range))

(defonce db-atom (atom (new-db)))

(defonce performance-data (atom []))

(defonce latest-result (atom nil))

(defonce zsxf-times (atom []))

(defn update-db-for-timeframe! [start-year end-year]
  (etl/populate-datascript-db @db-atom {:min-year start-year :max-year end-year}))

(def demo-query
  '[:find  ?p ?player-name (sum ?home-runs)
    :with ?year
    :where
    [?p :player/name ?player-name]
    [?season :season/player ?p]
    [?season :season/year ?year]
    [?season :season/home-runs ?home-runs]])

(defn query-datascript [db query]
  (let [timer (volatile! nil)
        result (util/time-f (d/q query db) #(vreset! timer %))]
    [(->> result
          (map rest)
          (sort-by second)
          reverse)
     @timer])) ; placeholder



(defn run-comparison-with-display [start-year end-year]
  (when (and (>= start-year min-year)
             (<= end-year max-year)
             (> end-year start-year))
    (reset! zsxf-times [])
    (update-db-for-timeframe! start-year end-year)
    (let [db @@db-atom
          db-size (if db (count (d/datoms db :eavt)) 0)
          [result1 time1] (query-datascript db demo-query)
          timer (volatile! nil)
          _ (util/time-f (q/get-result zquery) #(vreset! timer %))
          time2 @timer
          _(reduce + @zsxf-times)]

      (swap! performance-data conj
             {:timeframe (str start-year "-" end-year)
              :start-year start-year
              :end-year end-year
              :db-size db-size
              :engine1-time time1
              :engine2-time time2
              :timestamp (System/currentTimeMillis)})


      (let [result {:query-result result1
                    :database-size db-size
                    :engine1-time time1
                    :engine2-time time2
                    :timeframe (str start-year "-" end-year)}]
        (reset! latest-result result)
        result)

      )))

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
      
      (clerk/table {::clerk/page-size 8}
                   (clerk/use-headers (cons ["Player" "Home Runs"] query-result)))])))

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
                           {:field "engine1-time" :title "Datascript Query Time (ms)"}]}}

     {:mark {:type "point" :filled true :size 100 :color "#ff7f0e"}
      :encoding {:x {:field "db-size" :type "quantitative"}
                 :y {:field "engine2-time" :type "quantitative"}
                 :tooltip [{:field "timeframe" :title "Time Range"}
                           {:field "db-size" :title "DB Size"}
                           {:field "engine2-time" :title "ZSXF Query Time (ms)"}]}}]

    :resolve {:scale {:color "independent"}}
    :config {:legend {:orient "bottom"}}}))


(defn reset-to-initial []
  (reset! db-atom (new-db))
  (reset! year-ipt init-range)
  (reset! performance-data [])
  (reset! latest-result nil)
  (reset! zsxf-times [])
  )

(comment
  (reset-to-initial)
  (reset! db-atom (etl/fresh-conn))

  )
