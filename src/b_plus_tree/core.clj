(ns b-plus-tree.core
  "Primary functions for interacting with the B+ Tree."
  (:require [b-plus-tree io nodes util]
            [b-plus-tree.util :refer [dbg verbose]]))

(defn next-ptr
  "Returns the next pointer when searching the tree for key in node."
  ([key node raf]
     (loop [key-ptrs (dbg (b-plus-tree.nodes/key-ptrs node))]
       (when-let [[k ptr] (dbg (first key-ptrs))]
         (if (neg? (dbg (compare key k)))
           ptr
           (if-let [key-ptrs (next key-ptrs)]
             (recur key-ptrs)
             (-> node :children last)))))))

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
       (when (dbg (b-plus-tree.util/in? (:keys node) key))
         node)
       (recur key (next-node key node raf) raf))))

(defn find-record
  "Finds the record in leaf's children which goes to key, or nil if not found."
  ([key leaf raf]
     {:pre [leaf (b-plus-tree.nodes/leaf? leaf)]}
     (->> leaf
          b-plus-tree.nodes/key-ptrs
          (map (fn get-record [[k ptr]]
                 (when (= k key)
                   (b-plus-tree.io/read-node ptr raf))))
          (filter identity)
          first)))

(defn find-type
  "Returns the next node of the given type while searching the tree for key."
  ([key type node raf]
     (case (:type node)
       :leaf (find-record key node raf)
       :record nil
       (when-let [nxt (next-node key node raf)]
         (if (= type (:type nxt))
           nxt
           (recur key type nxt raf))))))

(defn find
  "Returns the value associated with key by traversing the entire tree, or
  nil if not found."
  ([key raf]
     (let [root (b-plus-tree.io/read-root raf)]
       (when-let [record (find-type key :record root raf)]
         (:data record)))))

(defn insert-record
  "Inserts a record into the given leaf node and writes changes to file.
  Returns the next free space."
  ([key val leaf next-free page-size raf]
     (let [[new-keys new-ptrs]
           (if-let [key-ptrs (seq (b-plus-tree.nodes/key-ptrs leaf))]
             (let [_ (println "key-ptrs" key-ptrs)
                   split-key-ptrs (split-with #(< % key))
                   new-key-ptrs (concat (first split-key-ptrs)
                                        [[key next-free]]
                                        (last split-key-ptrs))]
               (apply map list new-key-ptrs))
             [[key] [next-free]])
           new-leaf (assoc leaf :keys new-keys :children new-ptrs)
           record {:type :record, :data val, :offset next-free}]
       (println "leaf" new-leaf)
       (println "record" record)
       (println raf)
       (b-plus-tree.io/write-node new-leaf raf)
       (b-plus-tree.io/write-node record raf)
       (+ next-free page-size))))

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
               new-root (assoc root :next-free next-free)]
           (b-plus-tree.io/write-node new-root raf))
         (comment (do-insertion))))))

(defn traverse
  "Returns a lazy sequence of the key value pairs contained in the B+ Tree,
  assuming that leaf contains start, the key from which traversal begins.
  The sequence ends before stop is reached, or if no stop is given ends when
  there are no more leaves left.

  Not working."
  ([leaf start raf]
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
  ([leaf start stop raf]
     (take-while (fn [[k v]] (-> k (compare stop) neg?))
                 (traverse leaf start raf))))

(defn find-slice
  ""
  ([start raf]
     (when-let [leaf (find-type start :leaf (b-plus-tree.io/read-root raf) raf)]
       (println "leaf:" leaf)
       (traverse start leaf raf)))
  ([start stop raf]
     (when-let [leaf (find-type start :leaf (b-plus-tree.io/read-root raf) raf)]
       (println "leaf:" leaf)
       (traverse start stop leaf raf))))
