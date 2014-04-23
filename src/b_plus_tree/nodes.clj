(ns b-plus-tree.nodes
  "Defines encodings for the different types of nodes used in the B+ Tree."
  (:require [gloss core]
            [b-plus-tree.util :refer [dbg verbose]]))

(gloss.core/defcodec- node-types
  (gloss.core/enum :byte
                   :root-leaf :root-nonleaf :internal :leaf :record))

(gloss.core/defcodec- C-string
  (gloss.core/string :ascii :delimiters ["\0"]))

(gloss.core/defcodec- key-list
  (gloss.core/repeated C-string))

(gloss.core/defcodec- child-list
  (gloss.core/repeated :int64))

(gloss.core/defcodec root-leaf-node
  (gloss.core/ordered-map
   :type     :root-leaf
   :nextfree :int64
   :pagesize :int16
   :keys     key-list
   :children child-list))

(gloss.core/defcodec root-nonleaf-node
  (gloss.core/ordered-map
   :type     :root-nonleaf
   :nextfree :int64
   :pagesize :int16
   :keys     key-list
   :children child-list))

(gloss.core/defcodec internal-node
  (gloss.core/ordered-map
   :type     :internal
   :keys     key-list
   :children child-list))

(gloss.core/defcodec leaf-node
  (gloss.core/ordered-map
   :type     :leaf
   :keys     key-list
   :children child-list
   :nextleaf :int64))

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

(def leaf-types
  #{:root-leaf :leaf})

(defn key-ptrs
  "Given a node, returns key-ptr pairs, where each ptr points to the node
  which is less-than key, or in the case of leaf-nodes, contains key's value"
  ([node] (map list (:keys node) (:children node))))

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
     (let [num-children (-> node :children count)]
       (>= num-children (max-children node order)))))
