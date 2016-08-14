(ns b-plus-tree.io
  "Operations for B+ Tree I/O."
  (:require [gloss.core :as gloss]
            [gloss.io :as gio]
            [b-plus-tree.nodes :as nodes]
            [b-plus-tree.util :as util]
            [taoensso.timbre :as log]
            ))

(defn header-size
  []
     (gloss/byte-count
      (gio/encode nodes/header-node
                       {:count 0
                        :free 0
                        :order 0
                        :key-size 0
                        :val-size 0
                        :page-size 0
                        :root 0})))

(defn max-node-size
  ([order key-size]
     (gloss/byte-count
      (gio/encode nodes/node
                       {:type :internal
                        :key-ptrs (apply sorted-map
                                         (interleave
                                          (util/unique-strings (dec order)
                                                               key-size)
                                          (range)))
                        :last order}))))

(defn max-record-size
  ([val-size]
     (gloss/byte-count
      (gio/encode nodes/node
                       {:type :record
                        :data  (apply str
                                      (repeat val-size \a))}))))

(defn min-page-size
  ([order key-size val-size]
     (max (max-node-size order key-size)
          (max-record-size val-size)
          (header-size))))

(defn check-parameters
  ([order key-size val-size page-size]
     (>= page-size
         (min-page-size order key-size val-size))))

(defn new-tree
  "Creates a new file."
  ([filename order key-size val-size]
     (let [page-size (min-page-size order key-size val-size)]
       (new-tree filename order key-size val-size page-size)))
  ([filename order key-size val-size page-size]
     (when-not (check-parameters order key-size val-size page-size)
       (throw (ex-info "Insufficient page size.")))
     (let [header (gio/encode nodes/header-node
                                   {:count     0,
                                    :free      page-size,
                                    :order     order,
                                    :key-size  key-size,
                                    :val-size  val-size,
                                    :page-size page-size,
                                    :root      -1})]
       (with-open [raf (new java.io.RandomAccessFile filename "rwd")]
         (if (pos? (.length raf))
           (throw (ex-info "File already exists."))
           (.write raf
                   (.array (gio/contiguous header))))))))

(defn read-header
  "Reads the header from the RandomAccessFile."
  [^java.io.RandomAccessFile raf]
     (.seek raf 0) ; go to head of file
     (let [header-bytes (byte-array (header-size))]
       (.readFully raf header-bytes)
       (gio/decode nodes/header-node header-bytes)))

(defn write-header
  "Writes the header to the RandomAccessFile."
  [header ^java.io.RandomAccessFile raf]
  (log/info "Seeking to header")
   (.seek raf 0)
   (log/info "Writing header...")
   (.write raf
           (.array (gio/contiguous (gio/encode nodes/header-node
                                                         header)))))

(defn read-node
  "Reads the node stored in the RandomAccessFile at the given offset."
  [offset ^java.io.RandomAccessFile raf]
   (.seek raf offset)
   (let [size (.readShort raf)
         node-bytes (byte-array size)]
     (.readFully raf node-bytes)
     (assoc (gio/decode nodes/node (gio/to-byte-buffer node-bytes))
       :offset offset)))

(comment
  (defn read-root
    "Reads the root node from the RandomAccessFile"
    ([page-size raf]
       (if (zero? (.length raf))
         (b-plus-tree.nodes/new-root page-size)
         (read-node 0 raf)))))

(defn write-node
  "Writes the node to the RandomAccessFile at the given offset. Returns the
  offset of the file after writing."
  [{:keys [offset] :as node} ^java.io.RandomAccessFile raf]
     (let [encoded-node (gio/encode nodes/node node)
           size (gloss/byte-count encoded-node)]
      (log/infof "writing node %s" offset)
       (doto raf
         (.seek offset)
         (.writeShort size)
         (.write (.array (gio/contiguous encoded-node))))
      (log/infof "done writing node %s" offset)
       (.getFilePointer raf)))

(defn write-cache
  "Writes all nodes in the cache map which have been altered"
  [cache raf]
     (and (seq cache)
          (->> cache
               vals
               (filter :altered?)
               (map #(write-node % raf))
               doall)))

(comment
  (defn write-cache
    ([cache raf]
       (let [nodes (vals cache)
             altered-nodes (filter :altered? nodes)]
         (doall (map #(write-node % raf) altered-nodes))))))
