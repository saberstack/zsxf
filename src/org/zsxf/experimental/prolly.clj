(ns org.zsxf.experimental.prolly
  (:import (clojure.lang ILookup)
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

(definterface IProllyNode
  (^int nodeLevel [])
  (^objects nodeKeys [])
  (^objects nodeVals []))

(deftype ProllyLeaf [^int level ^objects keys ^objects vals]
  IProllyNode
  (nodeLevel [_this] level)
  (nodeKeys [_this] keys)
  (nodeVals [_this] vals)

  ILookup
  (valAt [this k] (.valAt this k nil))
  )

(deftype InternalNode [])

(deftype LeafNode [])
