(ns b-plus-tree.nodes
  "Defines encodings for the different types of nodes used in the B+ Tree."
  (:require [gloss core]
            [b-plus-tree.util :refer [dbg verbose]]))

(gloss.core/defcodec- C-string
  (gloss.core/string :ascii :delimiters ["\0"]))

(gloss.core/defcodec- key-list
  (gloss.core/repeated C-string))

(gloss.core/defcodec- child-list
  (gloss.core/repeated :int64))

(gloss.core/defcodec root-node
  (gloss.core/ordered-map
   :nextfree :int64
   :keys     key-list
   :children child-list))

(gloss.core/defcodec child-node
  (gloss.core/ordered-map
   :keys     key-list
   :children child-list))

(gloss.core/defcodec record-node
  {:data C-string})

(def type->encoding
  {:root-leaf    [0 root-node  ]
   :root-nonleaf [1 root-node  ]
   :internal     [2 child-node ]
   :leaf         [3 child-node ]
   :record       [4 record-node]})

(def byte->encoding
  {0 [:root-leaf    root-node  ]
   1 [:root-nonleaf root-node  ]
   2 [:internal     child-node ]
   3 [:leaf         child-node ]
   4 [:record       record-node]})

(def leaf-types
  #{:root-leaf :leaf})

(defn key-ptrs
  "Given a node, returns key-ptr pairs, where each ptr points to the node
  which is less-than key, or in the case of leaf-nodes, contains key's value"
  ([node] (dbg (map list (:keys node) (:children node)))))
