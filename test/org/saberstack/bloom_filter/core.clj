(ns org.saberstack.bloom-filter.core
  ;WIP
  (:require [clj-memory-meter.core :as mm]
            [criterium.core :as crit]
            [ham-fisted.bloom-filter :as ham.bf])
  (:import (ham_fisted BlockSplitBloomFilter)))

(defonce *bf (atom nil))

(defn init []
  (reset! *bf (ham.bf/bloom-filter 100000 0.05)))

(defn bf-insert [x]
  (let [bf (ham.bf/insert-hash! @*bf
             (ham.bf/hash-obj x))]
    bf))

(defn bf-contains? [x]
  (let [^BlockSplitBloomFilter bf @*bf]
    (.findHash bf (ham.bf/hash-obj x))))
