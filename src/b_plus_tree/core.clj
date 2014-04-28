(ns b-plus-tree.core
  "Primary functions for interacting with the B+ Tree."
  (:require [clojure.set :as set]
            [b-plus-tree io nodes util]
            [b-plus-tree.util :refer [dbg verbose]]))

(defn next-ptr
  "Returns the next pointer when searching the tree for key in node."
  ([key node raf]
     (let [key-ptrs (:key-ptrs node)]
       (if-let [ptr (loop [[k ptr]  (first key-ptrs)
                           key-ptrs (next key-ptrs)]
                      (let [c (compare key k)]
                        (cond
                         (pos? c) (when (seq key-ptrs)
                                    (recur (first key-ptrs)
                                           (next key-ptrs)))
                         (zero? c) (-> key-ptrs first second)
                         ; must be negative
                         :default ptr)))]
         ptr
         (:last node)))))

(defn next-node
  "Returns the next node when searching the tree for key in node."
  ([key node raf]
     (when-let [next-ptr (next-ptr key node raf)]
       (b-plus-tree.io/read-node next-ptr raf))))

(defn find-leaf
  "Recursively finds the leaf node associated with key by traversing node's
  subtree. If the given node is itself a leaf node, returns the node if it
  contains key, else returns nil."
  ([key node raf]
     (if (contains? b-plus-tree.nodes/leaf-types (:type node))
       (when (b-plus-tree.util/in? (:keys node) key)
         node)
       (recur key (next-node key node raf) raf))))

(defn find-record
  "Finds the record in leaf's children which goes to key, or nil if not found."
  ([key leaf raf] {:pre [leaf (b-plus-tree.nodes/leaf? leaf)]}
     (->> leaf
          :key-ptrs
          (map (fn get-record [[k ptr]]
                 (when (= k key)
                   (b-plus-tree.io/read-node ptr raf))))
          (filter identity)
          first)))

(defn find-type
  "Returns the next node of the given type while searching the tree for key."
  ([key types node raf]
     (cond
      (b-plus-tree.nodes/leaf? node)
      (when (b-plus-tree.util/in? types :record)
        (find-record key node raf))
      
      (= :record (:type node)) nil
      
      :default
      (when-let [nxt (next-node key node raf)]
        (if (b-plus-tree.util/in? types (:type nxt))
          nxt
          (recur key types nxt raf))))))

(defn find
  "Returns the value associated with key by traversing the entire tree, or
  nil if not found."
  ([key page-size raf]
     (let [root (b-plus-tree.io/read-root page-size raf)]
       (when-let [record (find-type key #{:record} root raf)]
         (:data record)))))

(defn insert-record
  "Inserts a record into the given leaf node and writes changes to file.
  Returns the next free space."
  ([key val leaf next-free page-size raf]
     (println "leaf:" leaf)
     (let [new-leaf (b-plus-tree.nodes/leaf-assoc key next-free leaf)
           record {:type :record, :data val, :offset next-free}]
       (println "new-leaf" new-leaf)
       (b-plus-tree.io/write-node new-leaf raf)
       (b-plus-tree.io/write-node record raf)
       (+ next-free page-size))))

; problem: I am re-writing the root on disc, but then using the same
; in-memory root every time
(defn insert
  "Inserts key-value pair into the B+ Tree. Returns the new record if
  successful, or nil if key already exists."
  ([key val order page-size raf]
     (let [root (b-plus-tree.io/read-root page-size raf)
           next-free (:next-free root)
           ; find the leaf to insert into, while building a stack of
           ; parent pointers
           [leaf stack]
           (loop [node      root
                  next-free next-free
                  stack     []]
             (let [stack (conj stack node)]
               (if (b-plus-tree.nodes/leaf? node)
                 ; found leaf
                 [node stack]
                 ; keep searching
                 (recur (next-node key node raf) next-free stack))))]
       (when-not (find-record key leaf raf)
         ; record doesn't exist already, so we can insert
         (let [next-free
               (if-not (b-plus-tree.nodes/full? leaf order)
                 (insert-record key val leaf next-free page-size raf)
                 ; placeholder
                 next-free)
               new-root (assoc (if (= :root-leaf (:type leaf))
                                 leaf
                                 root)
                          :next-free next-free)]
           (b-plus-tree.io/write-node new-root raf))))))

(defn traverse
  "Returns a lazy sequence of the key value pairs contained in the B+ Tree,
  assuming that leaf contains start, the key from which traversal begins.
  The sequence ends before stop is reached, or if no stop is given ends when
  there are no more leaves left.

  Not working."
  ([leaf start page-size raf]
     (let [next-fn
           (fn next-fn [leaf start raf found?]
             (let [next-ptr (:next-leaf leaf)]
               (if found?
                 (lazy-cat
                  (->> leaf
                       b-plus-tree.nodes/key-ptrs
                       (map (fn [[k ptr]]
                              [k (:data (b-plus-tree.io/read-node ptr raf))])))
                  (when (not= next-ptr -1)
                    (let [next-leaf (b-plus-tree.io/read-node next-ptr raf)]
                      (lazy-seq (next-fn next-leaf start raf true)))))
                 (when-let [pairs (->> leaf
                                   b-plus-tree.nodes/key-ptrs
                                   (filter #(-> % (compare start) neg? not))
                                   #(when-not found? (contains? % start)))]
                   (lazy-cat
                    (map (fn [[k ptr]]
                           [k (:data (b-plus-tree.io/read-node ptr raf))])
                         pairs)
                    (when (not= next-ptr -1)
                      (let [next-leaf (b-plus-tree.io/read-node next-ptr raf)]
                        (next-fn next-leaf start raf true))))))))]
       (lazy-seq (next-fn leaf start raf false))))
  ([leaf start stop page-size raf]
     (take-while (fn [[k v]] (-> k (compare stop) neg?))
                 (traverse leaf start page-size raf))))

(defn find-slice
  ""
  ([start page-size raf]
     (when-let [leaf (find-type start
                                :leaf
                                (b-plus-tree.io/read-root page-size raf)
                                raf)]
       (traverse start leaf page-size raf)))
  ([start stop page-size raf]
     (when-let [leaf (find-type start
                                :leaf
                                (b-plus-tree.io/read-root page-size raf)
                                raf)]
       (traverse start stop leaf page-size raf))))
