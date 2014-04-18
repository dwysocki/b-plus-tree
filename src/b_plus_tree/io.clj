(ns b-plus-tree.io
  "Operations for B+ Tree I/O."
  (:require [gloss core io]
            [b-plus-tree.nodes :as nodes]))

(defn seek
  ([^java.io.RandomAccessFile raf
    ^java.lang.long           offset]
     (.seek raf offset)))

