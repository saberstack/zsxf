(ns org.zsxf.demo.react
  (:require [charred.api :as charred]
            [clj-memory-meter.core :as mm]
            [datascript.core :as d]
            [taoensso.nippy :as nippy]
            [taoensso.nippy :as nippy]
            [net.cgrand.xforms :as xforms]
            [org.zsxf.datalog.compiler :refer [static-compile]]
            [org.zsxf.datascript :as ds]
            [org.zsxf.query :as q]
            #?(:clj [hato.websocket :as hws])
            [org.zsxf.xf :as xf]))


(def schema
  {:cell/id       {:db/cardinality :db.cardinality/one
                   :db/unique      :db.unique/identity}
   :zsxf.input/id {:db/cardinality :db.cardinality/one
                   :db/unique      :db.unique/identity}
   :cell/data     {}})

(defn gen-cells [n]
  (into []
    (comp
      (map (fn [id]
             {:cell/id   id
              :cell/data ["cell id" id]})))
    (range n)))

(defonce *conn (atom nil))

(defn init [n]
  (let [conn (d/create-conn schema)
        data (gen-cells n)]
    (reset! *conn conn)
    (d/transact! conn data)))

(defn get-cell-data [cell-id]
  (d/q
    '[:find ?d
      :in $ ?cell-id
      :where
      [?e :cell/id ?cell-id]
      [?e :cell/data ?d]]
    @@*conn
    cell-id))

(defn set-cell-data! [cell-id data]
  (d/transact! @*conn
    [{:cell/id   cell-id
      :cell/data data}]))

(comment

  (init 1000)

  (mm/measure *conn)

  (let [query-all (q/create-query
                    (static-compile
                      '[:find ?d
                        :where
                        [?e :cell/data ?d]
                        [?e :cell/id ?cell-id]
                        [?p :zsxf.input/id ?cell-id]]))]
    (def query-all query-all)
    (ds/init-query-with-conn query-all @*conn))

  (q/get-result query-all)

  (time
    (ds/get-result @*conn query-all (rand-int 1000)))

  (dotimes [_ 100]
    (time
      (ds/get-result @*conn query-all (rand-int 1000))))

  (dotimes [_ 100]
    (time
      (d/q
        '[:find ?d
          :in $ ?cell-id
          :where
          [?e :cell/id ?cell-id]
          [?e :cell/data ?d]]
        @@*conn
        (rand-int 1000))))


  (time
    (ds/get-result @*conn query-all 1000))

  (org.zsxf.alpha.repl/sample-indices query-all)

  (mm/measure query-all)

  (d/q
    '[:find ?cell-id
      :where
      [?e :cell/data ?d]
      [?e :cell/id ?cell-id]
      [?p :input/param ?param-value]
      [(= ?cell-id ?param-value)]]
    @@*conn)

  (time
    (d/transact! @*conn [{:input/param 3}]))

  (set-cell-data! 3 "new data")

  (dotimes [_ 10]
    (time
      (dotimes [cell-id 500]
        (get-cell-data cell-id))))

  (time
    (dotimes [cell-id 500]
      (set-cell-data! cell-id "500!")))

  )

#?(:clj
   (defn init-ws []
     (let [ws @(hws/websocket "ws://echo.websocket.events"
                 {:on-message (fn [ws msg last?]
                                (println "Received message:" msg))
                  :on-close   (fn [ws status reason]
                                (println "WebSocket closed!"))})]
       (hws/send! ws "Hello World!")
       (Thread/sleep 1000)
       (hws/close! ws))))

(defn coinbase-subscribe-msg []
  (charred/write-json-str
    {"product_ids" ["ETH-USD" "LTC-USD" "BTC-USD"]
     "type"        "subscribe"
     "channels"    [
                    "ticker"
                    ;"heartbeat"
                    ;{"product_ids" ["ETH-BTC" "ETH-USD"] "name" "ticker"}
                    ]}))


(defonce *messages (atom []))


(defn parse-messages []
  (transduce
    (comp
      (map str)
      (map (fn [s]
             (try
               (charred/read-json s :key-fn keyword)
               (catch Throwable e nil))))
      (filter :product_id))
    conj
    []
    @*messages))

(defn message-stats []
  (transduce
    (comp
      (map str)
      (map (fn [s]
             (try
               (charred/read-json s :key-fn keyword)
               (catch Throwable e nil))))
      (filter :product_id)
      (xforms/by-key
        :product_id
        (fn [item] item)
        (fn [k items]
          (if k {k {:count (count items)
                    ;:items items
                    }} {}))
        (xforms/into [])))
    conj
    []
    @*messages))

(defn on-message [ws msg last?]
  (swap! *messages conj msg))

(defn init-ds []
  (let [conn (d/create-conn {})]
    (def conn conn)
    (d/transact! conn (parse-messages))))

(defn trade-stats-ds [conn]
  (d/q
    '[:find ?currency (count ?te)
      :where
      [?te :product_id ?currency]]
    @conn))

(defn trade-stats-sell-ds [conn]
  (d/q
    '[:find ?currency (count ?te)
      :where
      [?te :side "buy"]
      [?te :product_id ?currency]]
    @conn))

(defn trade-currencies-ds [conn]
  (d/q
    '[:find ?currency
      :where
      [?te :product_id ?currency]]
    @conn))

#?(:clj
   (defn init-coinbase-ws []
     (let [ws @(hws/websocket "wss://ws-feed.exchange.coinbase.com"
                 {:on-message on-message
                  :on-close   (fn [ws status reason]
                                (println "WebSocket closed!"))})]
       (def ws ws)
       (hws/send! ws (coinbase-subscribe-msg)))
     ))

(comment

  (hws/close! ws)

  (do
    (init-ds)
    :done)


  (let [query (q/create-query
                (static-compile
                  '[:find ?currency
                    :where
                    [?te :side "buy"]
                    [?te :product_id ?currency]]))]
    (ds/init-query-with-conn query conn)
    (def query query)
    (q/get-result query))

  (time
    (d/q '[:find ?currency
           :where
           [?te :side "buy"]
           [?te :product_id ?currency]]
      @conn))

  (time (trade-currencies-ds conn))

  (time (trade-stats-ds conn))

  (time (trade-stats-sell-ds conn))

  (dotimes [_ 100]
    (time (trade-stats-ds conn)))

  (mm/measure @*messages)

  (mm/measure conn)

  (count @*messages)

  (message-stats)

  (count
    (nippy/thaw-from-file
      "resources/price_ticker/vector_of_maps.nippy"))

  )
