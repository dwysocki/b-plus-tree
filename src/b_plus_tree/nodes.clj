(ns b-plus-tree.nodes
  "Defines encodings for the different types of nodes used in the B+ Tree."
  (:require [gloss core]))

(def node-types
  [:root-leaf :root-nonleaf :internal :leaf :record])

(def node-type->id
  (into {} (map (fn [keyword id] [keyword (byte id)])
                node-types (range))))

(def id->node-type
  (into {} (map (fn [keyword id] [id keyword])
                node-type->id)))

(gloss.core/defcodec- key-list
  (repeated (string :ascii :length key-length)
            :prefix (prefix :ubyte
                            #(/ % key-length)
                            #(* % key-length))))

(gloss.core/defcodec- child-list
  (repeated :int64
            :prefix (prefix :ubyte
                            #(/ % 8)
                            #(* % 8))))

(gloss.core/defcodec- record-list
  (gloss.core/repeated (gloss.core/string :ascii :length val-length)
                       :prefix (gloss.core/prefix :ubyte
                                                  #(/ % val-length)
                                                  #(* % key-length))))

(gloss.core/defcodec root-leaf-node
  (gloss.core/ordered-map
   :keys key-list
   :vals val-list))

(gloss.core/defcodec root-non-leaf-node
  (gloss.core/ordered-map
   :keys key-list
   :children child-list))

(gloss.core/defcodec internal-node
  (gloss.core/ordered-map
   :keys key-list
   :children child-list))

(gloss.core/defcodec leaf-node
  (gloss.core/ordered-map
   :keys key-list
   :vals val-list))

(gloss.core/defcodec record-node
  (gloss.core/ordered-map
   :vals val-list))

(gloss.core/defcodec btree-node
  (gloss.core/header
   node-type
   {:root-leaf root-leaf-node, :root-non-leaf root-non-leaf-node,
    :internal  internal-node,  :leaf          leaf-node}
   :type))
