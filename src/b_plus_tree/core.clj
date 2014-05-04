(ns b-plus-tree.core
  "Primary functions for interacting with the B+ Tree."
  (:require [clojure.set :as set]
            [b-plus-tree io nodes util]
            [b-plus-tree.util :refer [dbg verbose]]))

(defn cache-node
  "Adds the node to the cache.

  Future plan:
    When cache exceeds some capacity, write entire cache to disc before
    proceeding, and clear cache."
  ([node raf cache]
     (assoc cache
       (:offset node) node)))

(defn cache-nodes
  "Adds the nodes to the cache."
  ([nodes raf cache]
     (if-let [node (first nodes)]
       (recur (next nodes)
              raf
              (assoc cache
                (:offset node) node))
       cache)))

(defn get-node
  "Reads the node from disc only if it is not already in the cache"
  ([ptr raf cache]
     (if-let [node (cache ptr)]
       [node cache]
       (let [node (b-plus-tree.io/read-node ptr raf)]
         [node (cache-node node raf cache)]))))

(defn next-ptr
  "Returns the next pointer when searching the tree for key in node."
  ([key node]
     (let [[k ptr] (->> node
                        :key-ptrs
                        (filter (fn [[k ptr]] (pos? (compare k key))))
                        first)]
       (or ptr (:last node)))))

(defn next-node
  "Returns the next node when searching the tree for key in node."
  ([key node raf
    & {:keys [cache]
       :or {cache {}}}]
     (if-let [next-ptr (next-ptr key node)]
       (get-node next-ptr raf cache)
       [nil cache])))

(defn find-leaf
  "Recursively finds the leaf node associated with key by traversing node's
  subtree. If the given node is itself a leaf node, returns the node if it
  contains key, else returns nil."
  ([key node raf]
     (if (contains? b-plus-tree.nodes/leaf-types (:type node))
       (when (b-plus-tree.util/in? (:keys node) key)
         node)
       (recur key (next-node key node raf) raf))))


(defn find-record2
  "Finds the record in leaf's children which goes to key, or nil if not found."
  ([key leaf raf
    & {:keys [cache]
       :or {cache {}}}] {:pre [leaf (b-plus-tree.nodes/leaf? leaf)]}
       (->> leaf
            :key-ptrs
            (map (fn get-record [[k ptr]]
                   (when (= k key)
                     (get-node ptr raf cache))))
            (filter identity)
            first)))

(defn find-record
  "Finds the record in leaf's children which goes to key, or nil if not found."
  ([key leaf raf
    & {:keys [cache]
       :or {cache {}}}] {:pre [leaf (b-plus-tree.nodes/leaf? leaf)]}
       (let [found
             (->> leaf
                  :key-ptrs
                  (map (fn get-record [[k ptr]]
                         (when (= k key)
                           (get-node ptr raf cache))))
                  (filter identity)
                  first)]
         (or found
             [nil cache]))))



(defn find-type
  "Returns the next node of the given type while searching the tree for key."
  ([key types node raf
    & {:keys [cache]
       :or {cache {}}}]
     (cond
      (b-plus-tree.nodes/leaf? node)
      (if (b-plus-tree.util/in? types :record)
        (find-record key node raf
                     :cache cache)
        [nil cache])
      
      (= :record (:type node)) [nil cache]
      
      :default
      (if-let [[nxt cache] (next-node key node raf :cache cache)]
        (if (b-plus-tree.util/in? types (:type nxt))
          [nxt cache]
          (recur key types nxt raf
                 {:cache cache}))))))

(defn find-type-stack
  "Returns the next node of the given type while searching the tree for key.
  Builds a stack of visited nodes during the process."
  ([key types node stack raf
    & {:keys [cache]
       :or {cache {}}}]
     (let [stack (conj stack node)]
       (cond
        (b-plus-tree.nodes/leaf? node)
        (if (b-plus-tree.util/in? types :record)
          (let [[data cache]
                (find-record key node raf
                             :cache cache)]
            [data stack cache])
          [nil stack cache])

        (= :record (:type node)) [nil stack cache]

        :default
        (if-let [[nxt cache] (next-node key node raf :cache cache)]
          (if (b-plus-tree.util/in? types (:type nxt))
            [nxt stack cache]
            (recur key types nxt stack raf
                   {:cache cache})))))))

(defn find
  "Returns the value associated with key by traversing the entire tree, or
  nil if not found."
  ([key raf {cnt :count, size :key-size, root-ptr :root :as header} &
    {:keys [cache]
     :or {cache {}}}]
     (if-not (or (zero? cnt)
                   (> (count key) size))
       (let [[root cache] (get-node root-ptr raf cache)
             [record cache] (find-type key #{:record} root raf
                                       :cache cache)]
         [(when record (:data record)), cache])
       [nil, cache])))

(defn find-stack
  "Returns the value associated with key by traversing the entire tree, or
  nil if not found, building a stack of visited nodes during the process."
  ([key raf {cnt :count, size :key-size, root-ptr :root :as header} &
    {:keys [cache]
     :or {cache {}}}]
     (if-not (or (zero? cnt)
                   (> (count key) size))
       (let [[root cache] (get-node root-ptr raf cache)
             [record stack cache]
             (find-type-stack key #{:record} root [] raf
                              :cache cache)]
;         (println "herp" [record stack cache])
         [(when record (:data record)), stack, cache])
       [nil, [], cache])))

(defn insert-record
  "Inserts a record into the given leaf node and writes changes to file.
  Returns the next free space."
  ([key val leaf next-free page-size raf]
;     (println "leaf:" leaf)
     (let [new-leaf (b-plus-tree.nodes/leaf-assoc key next-free leaf)
           record {:type :record, :data val, :offset next-free}]
       (println "new-leaf" new-leaf)
       (b-plus-tree.io/write-node new-leaf raf)
       (b-plus-tree.io/write-node record raf)
       (+ next-free page-size))))

(defn insert
  "Inserts a key-value pair into the B+ Tree. Returns a vector whose first
  element is the new header, and whose second element is a cache map, which
  maps pointer offsets to the nodes located there, for all nodes which have
  been read.
  If a node is altered, it is given an additional [:altered? true] entry."
  ([key val raf
    {size :count, root-ptr :root
     :keys [free order key-size val-size page-size]
     :as header}
    & {:keys [cache]
       :or {cache {}}}]
     {:pre [(>= key-size (count key))
            (>= val-size (count val))]}
     (if (zero? size)
       (let [[root record] (b-plus-tree.nodes/new-root key val free page-size)]
         [(assoc header
            :count 1
            :root  (:offset root)
            :free  (+ (:offset record) page-size)),
          (cache-nodes [root record]
                       raf
                       cache)])
       (let [[root cache] (get-node root-ptr raf cache)
             [record stack cache] (find-stack key raf header
                                              :cache cache)
             leaf (last stack)]
         (cond
          ; record already exists, do nothing
          record ;[header, cache]
          (throw (ex-info "repeat" {}))

          ; leaf is full, split
          (b-plus-tree.nodes/full? leaf order)
          (throw (new UnsupportedOperationException))

          ; leaf is not full, simple insert
          :default
          (let [leaf (b-plus-tree.nodes/leaf-assoc key free leaf)
                record {:type :record
                        :data val
                        :offset free
                        :altered? true}
                header (assoc header
                         :free (+ free page-size)
                         :count (inc size))]
            [header, (cache-nodes [leaf record]
                                  raf
                                  cache)]))))))

(defn insert-all
  "Inserts all key-vals from a map into the tree."
  ([keyvals raf header
    & {:keys [cache]
       :or {cache {}}}]
     (if-let [entry (first keyvals)]
       (let [[key val] entry
             [header cache] (b-plus-tree.core/insert key val raf
                                                     header
                                                     :cache cache)]
         (recur (next keyvals) raf header {:cache cache}))
       [header cache])))

; problem: I am re-writing the root on disc, but then using the same
; in-memory root every time
(comment
  (defn insert
    "Inserts key-value pair into the B+ Tree. Returns the new record if
  successful, or nil if key already exists."
    ([key val order page-size raf]
       (let [root (b-plus-tree.io/read-root page-size raf)
             free (:free root)
             ; find the leaf to insert into, while building a stack of
             ; parent pointers
             [leaf stack]
             (loop [node  root
                    stack []]
               (let [stack (conj stack node)]
                 (if (b-plus-tree.nodes/leaf? node)
                   ; found leaf
                   [node stack]
                   ; keep searching
                   (recur (next-node key node raf) stack))))]
         (when-not (find-record key leaf raf)
           ; record doesn't exist already, so we can insert
           (let [free
                 (if-not (b-plus-tree.nodes/full? leaf order)
                   (insert-record key val
                                  (assoc leaf
                                    :free free)
                                  free page-size raf)
                   ; placeholder
                   free)
                 new-root (assoc (if (= :root-leaf (:type leaf))
                                   leaf
                                   root)
                            :free free)]
             (when-not (= :root-leaf (:type leaf))
               (b-plus-tree.io/write-node (assoc root
                                            :free free)))))))))

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

(comment "work in progress"
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
                (traverse start stop leaf page-size raf)))))
