(ns b-plus-tree.nodes
  "Defines encodings for the different types of nodes used in the B+ Tree."
  (:require [gloss core]
            [b-plus-tree.util :refer [dbg verbose]]))

(gloss.core/defcodec- node-types
  (gloss.core/enum :byte
                   :root-leaf :root-nonleaf :internal :leaf :record))

(gloss.core/defcodec- raf-offset :int64)

(gloss.core/defcodec- C-string
  (gloss.core/string :ascii
                     :delimiters ["\0"]))

(gloss.core/defcodec- node-keys
  (gloss.core/repeated C-string))

(gloss.core/defcodec- node-ptrs
  (gloss.core/repeated raf-offset))

(defn- node-map
  "Turns a node's :keys and :ptrs into a map :key-ptrs, removing the original
  fields."
  ([{:keys [keys ptrs] :as node}]
     (-> node
         (assoc :key-ptrs (into (sorted-map) (zipmap keys ptrs)))
         (dissoc :keys :ptrs))))

(defn- node-unmap
  "Turns a node's :key-ptrs into :keys and :ptrs, removing the original field."
  ([{:keys [key-ptrs] :as node}]
     (-> node
         (assoc :keys (keys key-ptrs)
                :ptrs (vals key-ptrs))
         (dissoc :key-ptrs))))

(def root-leaf-node
  (gloss.core/compile-frame
   (gloss.core/ordered-map
    :type :root-leaf
    :free raf-offset
    :keys node-keys
    :ptrs node-ptrs)
   node-unmap
   node-map))

(def root-nonleaf-node
  (gloss.core/compile-frame
   (gloss.core/ordered-map
    :type :root-nonleaf
    :free raf-offset
    :keys node-keys
    :ptrs node-ptrs
    :last raf-offset)
   node-unmap
   node-map))

(def internal-node
  (gloss.core/compile-frame
   (gloss.core/ordered-map
    :type :internal
    :keys node-keys
    :ptrs node-ptrs
    :last raf-offset)
   node-unmap
   node-map))

(def leaf-node
  (gloss.core/compile-frame
   (gloss.core/ordered-map
    :type :leaf
    :keys node-keys
    :ptrs node-ptrs
    :next raf-offset)
   node-unmap
   node-map))

(gloss.core/defcodec record-node
  (gloss.core/ordered-map
   :type :record
   :data C-string))

(gloss.core/defcodec node
  (gloss.core/header node-types
                     {:root-leaf    root-leaf-node
                      :root-nonleaf root-nonleaf-node
                      :internal     internal-node
                      :leaf         leaf-node
                      :record       record-node}
                     :type))

(defn new-root
  "Returns a new leaf root."
  ([page-size]
     {:type      :root-leaf,
      :next-free page-size,
      :key-ptrs  (sorted-map),
      :offset    0}))

(def leaf-types #{:root-leaf, :leaf})

(defn leaf?
  "Returns true if node is a leaf-type, else nil."
  ([node] (b-plus-tree.util/in? leaf-types (:type node))))

(defn leaf-assoc
  "Given a leaf node, returns that node with key and ptr inserted at the
  correct position in :keys and :children."
  {:arglists '([key ptr leaf])}
  ([key ptr {:keys [key-ptrs] :as leaf}]
     (assoc leaf :key-ptrs (assoc key-ptrs key ptr))))

(defn count-children
  "Returns the number of children node has"
  ([node]
     (let [N (count (:key-ptrs node))]
       (if (:last node)
         (inc N)
         N))))

(defn min-children
  "Returns the minimum number of children a given node is allowed to have."
  ([node order]
     (case (:type node)
       :root-leaf    1
       :root-nonleaf 2
       :internal     (-> order (/ 2) Math/ceil)
       :leaf         (-> order (/ 2) Math/floor)
       nil)))

(defn max-children
  "Returns the maximum number of children a given node is allowed to have."
  ([node order]
     (case (:type node)
       :root-leaf    order
       :root-nonleaf order
       :internal     order
       :leaf         (dec order)
       nil)))

(defn full?
  "Returns true if the node is full."
  ([node order]
     (>= (-> node :key-ptrs count)
         (max-children node order))))
