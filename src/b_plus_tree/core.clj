(ns b-plus-tree.core
  "Primary functions for interacting with the B+ Tree."
  (:require [b-plus-tree io nodes]
            [b-plus-tree.util :refer [dbg verbose]]))


(defn next-node
  "Returns the next node when searching the tree for key in node.
  Returns nil if node is a leaf."
  ([key node raf]
     (loop [key-ptrs (dbg (b-plus-tree.nodes/key-ptrs node))]
       (let [[k ptr] (dbg (first key-ptrs))]
         (if (pos? (dbg (.compareTo key k)))
           (if-let [key-ptrs (next key-ptrs)]
             (recur key-ptrs)
             (b-plus-tree.io/read-node raf (last (:children node))))
           (b-plus-tree.io/read-node raf ptr))))))

(defn find-leaf
  "Recursively finds the leaf node associated with key by traversing node's
  subtree. If the given node is itself a leaf node, returns the node if it
  contains key, else returns nil."
  ([key node raf]
     (if (contains? b-plus-tree.nodes/leaf-types (:type node))
       (if ((:keys node) key)
         node
         nil)
       (recur key (next-node key node raf) raf))))

(defn find-record
  "Finds the record node associated with key by traversing node's subtree.
  If key is not contained in node's subtree, returns nil."
  ([key node raf]
     (when-let [leaf (find-leaf key node raf)]
       (let [key-ptrs (b-plus-tree.nodes/key-ptrs leaf)]
         (loop [key-ptrs key-ptrs]
           (let [[k ptr] (first key-ptrs)]
             (if (= k key)
               (b-plus-tree.io/read-node raf ptr)
               (when-let [key-ptrs (next key-ptrs)]
                 (recur key-ptrs)))))))))

(defn find
  "Returns the value associated with key by traversing the entire tree, or
  nil if not found."
  ([key raf]
     (let [root (b-plus-tree.io/read-node raf 0)]
       (when-let [record (find-record key root raf)]
         (println record)
         (:data record)))))
