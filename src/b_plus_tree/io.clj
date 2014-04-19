(ns b-plus-tree.io
  "Operations for B+ Tree I/O."
  (:require [gloss core io]
            [b-plus-tree.nodes :as nodes]))

(defn read-page
  "Reads a page's worth of bytes from the RandomAccessFile at a given offset."
  ([raf offset pagesize]
     (let [b (byte-array pagesize)]
       (seek raf offset)
       (.readFully raf b)
       b)))

(defn read-node
  "Reads the node stored in the RandomAccessFile at the given offset."
  ([raf offset]
     (.seek raf offset)
     (let [type (.readByte raf)
           [type encoding] (nodes/byte->encoding type)
           size (.readShort raf)
           node-bytes (byte-array size)]
       (.readFully raf node-bytes)
       (-> node-bytes count println)
       (assoc (gloss.io/decode encoding (gloss.io/to-byte-buffer node-bytes))
         :type type))))

(defn write-node
  "Writes the node to the RandomAccessFile at the given offset. Returns the
  offset of the file after writing."
  ([node raf offset]
     (.seek raf offset)
     (let [[type encoding] (nodes/type->encoding (:type node))
           encoded-node (gloss.io/encode encoding node)
           size (gloss.core/byte-count encoded-node)]
       (doto raf
         (.writeByte type)
         (.writeShort size)
         (.write (.array (gloss.io/contiguous encoded-node))))
       (.getFilePointer raf))))
