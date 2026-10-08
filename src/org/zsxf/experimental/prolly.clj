(ns org.zsxf.experimental.prolly
  (:require [clojure.core.rrb-vector.nodes :refer [first-child]])
  (:import (clojure.lang ILookup IPersistentMap MapEntry Seqable)
           (java.security MessageDigest)))

;WIP

(defprotocol IBlockStore
  (put-block [this cid bytes])
  (get-block [this cid]))

(defn hash-bytes ^bytes [^bytes b]
  (.digest (MessageDigest/getInstance "SHA-256") b))

(defn boundary? [^bytes key-bytes]
  ;; Uses a mocked boundary condition. In production, this should be a
  ;; BuzHash rolling window over a continuous stream of keys.
  (zero? (bit-and (first (hash-bytes key-bytes)) 0x3F)))

;; =============================================================================
;; Node interfaces & deftypes
;; =============================================================================

(definterface IProllyNode
  ;TODO type hints
  (nodeLevel [])
  (nodeKeys [])
  (nodeVals []))

(deftype LeafNode [^int level ^objects keys ^objects vals]
  IProllyNode
  (nodeLevel [_this] level)
  (nodeKeys [_this] keys)
  (nodeVals [_this] vals)

  ILookup
  (valAt [_this k] (.valAt _this k nil))
  (valAt [_this k not-found]
    ;TODO
    )

  Seqable
  (seq [_]
    (let [len (alength keys)]
      (when (< 0 len)
        (map #(MapEntry/create (aget keys %) (aget vals %)) (range len)))))
  )

(deftype InternalNode [^int level ^objects keys ^objects children-content-ids]
  IProllyNode
  (nodeLevel [_this] level)
  (nodeKeys [_this] keys)
  (nodeVals [_this] children-content-ids))

;; =============================================================================
;; Serialization
;; =============================================================================

(defn serialize-node ^bytes [^IProllyNode node]
  ;; TODO convert node instance -> deterministic byte array (e.g., via Nippy)
  ;; mock
  #_(byte-array 0))

(defn deserialize-node [^bytes b]
  ;; TODO  Read bytes -> return instantiated prolly LeafNode or InternalNode
  ;; mock
  #_(LeafNode. 0 (object-array []) (object-array [])))

(defn build-tree [store sorted-kv-pairs]
  ;; TODO tree building
  ;;mock
  )

(deftype ProllyTree [store root-content-id]
  Seqable
  (seq [_]
    ;TODO traverse the leftmost children to the leaves
    (when root-content-id
      (let [node (deserialize-node (get-block store root-content-id))]
        (seq ^LeafNode node))))

  IPersistentMap
  (assoc [this k v]
    (let [current-seq (or (seq this) [])
          new-seq     (->>
                        (conj current-seq [k v])
                        (into {})
                        (sort-by first))]
      (ProllyTree. store (build-tree store new-seq)))))

;TODO NEXT: streaming chunker
