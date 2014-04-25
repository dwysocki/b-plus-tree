(ns b-plus-tree.nodes
  "Defines encodings for the different types of nodes used in the B+ Tree."
  (:require [gloss core]
            [b-plus-tree.util :refer [dbg verbose]]))

(gloss.core/defcodec- node-types
  (gloss.core/enum :byte
                   :root-leaf :root-nonleaf :internal :leaf :record))

(gloss.core/defcodec- raf-offset
  :int64)

(gloss.core/defcodec- C-string
  (gloss.core/string :ascii :delimiters ["\0"]))

(gloss.core/defcodec- key-val
  [C-string raf-offset])

(gloss.core/defcodec- key-vals
  (gloss.core/repeated key-val
                       :delimiters ["\n"]))

(gloss.core/defcodec root-leaf-node
  (gloss.core/ordered-map
   :type      :root-leaf
   :next-free raf-offset
   :key-ptrs  key-vals))

(gloss.core/defcodec root-nonleaf-node
  (gloss.core/ordered-map
   :type      :root-nonleaf
   :next-free raf-offset
   :key-ptrs  key-vals
   :last      raf-offset))

(gloss.core/defcodec internal-node
  (gloss.core/ordered-map
   :type     :internal
   :key-ptrs  key-vals
   :last      raf-offset))

(gloss.core/defcodec leaf-node
  (gloss.core/ordered-map
   :type      :leaf
   :key-ptrs  key-vals
   :next      raf-offset))

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
      :next-free  page-size,
      :key-ptrs          [], ; might have to be [[]] instead
      :offset             0}))

(def leaf-types #{:root-leaf :leaf})

(defn leaf?
  "Returns true if node is a leaf-type, else nil."
  ([node] (b-plus-tree.util/in? leaf-types (:type node))))

(defn leaf-assoc
  "Given a leaf node, returns that node with key and ptr inserted at the
  correct position in :keys and :children."
  {:arglists '([key ptr leaf])}
  ([key ptr {:keys [key-ptrs] :as leaf}]
     (assoc leaf :key-ptrs (assoc key-ptrs key ptr))))

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
