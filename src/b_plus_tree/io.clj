(ns b-plus-tree.io
  "Operations for B+ Tree I/O."
  (:require [gloss core io]
            [b-plus-tree.nodes :as nodes]
            [b-plus-tree.util :refer [dbg verbose]]))

(defn read-node
  "Reads the node stored in the RandomAccessFile at the given offset."
  ([offset raf]
     (.seek raf offset)
     (let [size (.readShort raf)
           _ (println "read-size:" size)
           node-bytes (doto (byte-array size)
                        #(.readFully raf %))
           node (gloss.io/decode nodes/node
                                 (gloss.io/to-byte-buffer node-bytes))]
       (assoc node :offset offset))))

(defn read-root
  "Reads the root node from the RandomAccessFile"
  ([page-size raf]
     (if (zero? (.length raf))
       (b-plus-tree.nodes/new-root page-size)
       (read-node 0 raf))))

(defn write-node
  "Writes the node to the RandomAccessFile at the given offset. Returns the
  offset of the file after writing."
  ([node raf]
     (let [offset (:offset node)
           encoded-node (gloss.io/encode nodes/node node)
           size (gloss.core/byte-count encoded-node)]
       (println "write-size:" size)
       (println "array-size" (-> encoded-node
                                 gloss.io/contiguous
                                 .array
                                 gloss.core/byte-count))
       (comment
         (doall
          (map println
               ["offset" "encoded" "size"]
               [offset encoded-node size])))
       (doto raf
         (.seek offset)
         (.writeShort size)
         (.write (.array (gloss.io/contiguous encoded-node))))
       (.getFilePointer raf))))
