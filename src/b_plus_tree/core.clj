(ns b-plus-tree.core
  "Primary functions for interacting with the B+ Tree."
  (:require [clojure.set :as set]
            [b-plus-tree io nodes seq util]
            [b-plus-tree.util :refer [dbg verbose]]))

(defn- cache-node
  "Adds the node to the cache.

  Future plan:
    When cache exceeds some capacity, write entire cache to disc before
    proceeding, and clear cache."
  ([node raf cache]
     (assoc cache
       (:offset node) node)))

(defn- cache-nodes
  "Adds the nodes to the cache."
  ([nodes raf cache]
     (if-let [node (first nodes)]
       (recur (next nodes)
              raf
              (assoc cache
                (:offset node) node))
       cache)))

(defn- get-node
  "Reads the node from disc only if it is not already in the cache"
  ([ptr raf cache]
     (if (or (nil? ptr) (neg? ptr))
       ; ptr is nil or negative, meaning it points to nothing
       [nil cache]
       (if-let [node (cache ptr)]
         [node cache]
         (let [node (b-plus-tree.io/read-node ptr raf)]
           [node (cache-node node raf cache)])))))

(defn- next-ptr
  "Returns the next pointer when searching the tree for key in node."
  ([key node]
     (let [[k ptr] (->> node
                        :key-ptrs
                        (filter (fn [[k ptr]] (pos? (compare k key))))
                        first)]
       (or ptr (:last node)))))

(defn- next-node
  "Returns the next node when searching the tree for key in node."
  ([key node raf
    & {:keys [cache]
       :or {cache {}}}]
     (if-let [next-ptr (next-ptr key node)]
       (get-node next-ptr raf cache)
       [nil cache])))

(defn- find-record
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



(defn- find-type
  "Returns the next node of the given type while searching the tree for key."
  ([key types node raf
    & {:keys [cache]
       :or {cache {}}}]
;     (println "NODE:" node)
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

(defn- find-type-stack
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

(defn find-val
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

(defn- find-stack
  "Returns the record associated with key by traversing the entire tree, or
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
         [record, stack, cache])
       [nil, [], cache])))

(defn- split-root-leaf
  "Splits a :root-leaf node. Do not attempt to split another type of node."
  ([{:keys [key-ptrs offset]
     :as root-leaf}
    raf
    {:keys [free page-size] :as header}
    & {:keys [cache]
       :or {cache {}}}]
     (let [[left right] (b-plus-tree.seq/split-half-into (sorted-map)
                                                         key-ptrs),
           [left-offset right-offset free]
           (b-plus-tree.seq/n-range free 3 page-size),
           
           left-node {:type :leaf
                      :key-ptrs left
                      :prev -1
                      :next right-offset
                      :offset left-offset
                      :altered? true},
           right-node {:type :leaf
                       :key-ptrs right
                       :prev left-offset
                       :next -1
                       :offset right-offset
                       :altered? true},
           root-node {:type :root-nonleaf
                      :key-ptrs (sorted-map (-> right first first)
                                            left-offset)
                      :last right-offset
                      :offset offset
                      :altered? true},
           header (assoc header
                    :free free),
           cache (cache-nodes [left-node right-node root-node]
                              raf
                              cache)]
       [header, cache])))

(defn- split-root-nonleaf
  "Splits a :root-nonleaf node. Do not attempt to split another type of node."
  ([{:keys [key-ptrs last offset]
     :as root-nonleaf}
    raf
    {:keys [free page-size] :as header}
    & {:keys [cache]
       :or {cache {}}}]
     (let [[left-kps [mid-k mid-p] right-kps]
           (b-plus-tree.seq/split-center key-ptrs),
           
           [left right] (map (partial into (sorted-map)) [left-kps right-kps]),
           
           [left-offset right-offset free]
           (b-plus-tree.seq/n-range free 3 page-size),

           left-node  {:type     :internal
                       :key-ptrs left
                       :last     mid-p
                       :offset   left-offset
                       :altered? true},
           right-node {:type     :internal
                       :key-ptrs right
                       :last     last
                       :offset   right-offset
                       :altered? true},
           root-node  {:type     :root-nonleaf
                       :key-ptrs (sorted-map mid-k left-offset)
                       :last     right-offset
                       :offset   offset
                       :altered? true},
           header     (assoc header
                        :free free),
           cache      (cache-nodes [left-node right-node root-node]
                                   raf
                                   cache)]
       [header, cache])))

(defn- split-internal
  "Splits an :internal node. Do not attempt to split another type of node.
  Returns [[k p] header cache], where k is the key to push to the parent,
  and p is the pointer of the new (right) node."
  ([{:keys [key-ptrs last offset]
     :as leaf}
    raf
    {:keys [free page-size] :as header}
    & {:keys [cache]
       :or {cache {}}}]
     (let [[left-kps [mid-k mid-p] right-kps]
           (b-plus-tree.seq/split-center key-ptrs),

           [left right] (map (partial into (sorted-map)) [left-kps right-kps]),

           [right-offset free]
           (b-plus-tree.seq/n-range free 2 page-size),

           left-node  {:type     :internal
                       :key-ptrs left
                       :last     mid-p
                       :offset   offset
                       :altered? true},
           right-node {:type     :internal
                       :key-ptrs right
                       :last     last
                       :offset   right-offset
                       :altered? true},
           header     (assoc header
                        :free free)
           cache      (cache-nodes [left-node right-node]
                                   raf
                                   cache)]
       [header, cache, [mid-k right-offset]])))

(defn- split-leaf
  "Splits a :leaf node. Do not attempt to split another type of node.
  Returns [[k p] header cache], where k is the key to add to the parent,
  and p is the pointer of the new (right) node."
  ([{:keys [key-ptrs next prev offset]
     :as leaf}
    raf
    {:keys [free page-size] :as header}
    & {:keys [cache]
       :or {cache {}}}]
     (let [[left right] (b-plus-tree.seq/split-half-into (sorted-map)
                                                         key-ptrs),
           [right-offset free] (b-plus-tree.seq/n-range free 2 page-size),

           left-node  {:type     :leaf
                       :key-ptrs left
                       :prev     prev
                       :next     right-offset
                       :offset   offset
                       :altered? true},
           right-node {:type     :leaf
                       :key-ptrs right
                       :prev     offset
                       :next     next
                       :offset   right-offset
                       :altered? true},
           raised-key (-> right first first),
           header     (assoc header
                        :free free)
           cache      (cache-nodes [left-node right-node]
                                   raf
                                   cache)]
       [header, cache, [raised-key right-offset]])))

(defn- split-node
  "Splits any type of node."
  ([{:keys [type] :as node} & rest]
     (apply (case type
              :root-leaf    split-root-leaf
              :root-nonleaf split-root-nonleaf
              :internal     split-internal
              :leaf         split-leaf
              (throw (ex-info "Invalid node" {})))
            node rest)))

(defn- insert-parent
  "Inserts a new key and pointer into a parent node, and returns the node."
  ([key ptr
    {:keys [key-ptrs last]
     :as parent}]
     (if-let [entry (->> key-ptrs
                         (drop-while (fn [[k p]] (pos? (compare key k))))
                         first)]
       ; there is an entry [k p] which comes after key in the sequence,
       ; so we must make the new key point to p, and k point to the
       ; new ptr
       (let [[k p]    entry
             key-ptrs (assoc key-ptrs
                        key p
                        k ptr)]
         (assoc parent
           :key-ptrs key-ptrs
           :altered? true))
       ; key is the last entry in the sequence, so we must make it
       ; point to what :last currently points to, and make :last point
       ; to the new ptr
       (let [key-ptrs (assoc key-ptrs
                        key last)]
         (assoc parent
           :key-ptrs key-ptrs
           :last     ptr
           :altered? true)))))

(defn- insert-split
  ([node stack raf
    {:keys [order]
     :as header}
    & {:keys [cache]
       :or {cache {}}}]
     (let [[header, cache, [key ptr]]
           (split-node node raf header
                       :cache cache)]
       (if (seq stack)
         ; push up key to parent
         (let [[stack parent] (b-plus-tree.seq/pop-stack stack)
               parent         (insert-parent key ptr parent)
               cache          (cache-node parent raf cache)]
           (if (b-plus-tree.nodes/overfull? parent order)
             ; parent is full, split parent
             (recur parent stack raf header {:cache cache})
             ; finished splitting
             [header, cache]))
         ; nothing left to split
         [header, cache]))))

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
       ; empty B+ Tree
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
             [stack leaf] (b-plus-tree.seq/pop-stack stack)]
         (if record
           ; record already exists, overwrite
           [header, (cache-node (assoc record
                                  :data val
                                  :altered? true)
                                raf
                                cache)]
           ; record does not exist, insert at leaf
           (let [leaf (b-plus-tree.nodes/leaf-assoc key free leaf)
                 record {:type :record
                         :data val
                         :offset free
                         :altered? true}
                 header (assoc header
                          :free (+ free page-size)
                          :count (inc size))]
             (if (b-plus-tree.nodes/overfull? leaf order)
               ; leaf is full, begin splitting
               (insert-split leaf stack raf header
                             :cache (cache-node record raf cache))
               ; leaf has room, return
               [header, (cache-nodes [leaf record]
                                     raf
                                     cache)])))))))

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

(defn- replace-keys
  "Takes a map of existing-key->replacement-key, and traverses up the stack
  until all existing keys have been replaced by their replacement keys.
  Returns the cache.
  Throws an exception if a key is not found in the stack."
  ([key-replacements stack raf cache]
     (let [[stack
            {:keys [key-ptrs]
             :as node}]
           (b-plus-tree.seq/pop-stack stack)
           
           ; set of all keys which will be replaced
           replaced-keys (apply clojure.set/intersection
                                (map (comp set keys)
                                     [key-replacements key-ptrs]))
           _ (println "replaced keys:" replaced-keys)
           
           updated-key-ptrs (clojure.set/rename-keys key-ptrs
                                                     key-replacements)
           node (if (seq replaced-keys)
                  ; keys were replaced
                  (assoc node
                    :key-ptrs updated-key-ptrs
                    :altered? true)
                  ; keys were not replaced
                  node)
           _ (println "altered?" (:altered? node))
           ; remove entries from key-replacements which have been used
           key-replacements (apply dissoc key-replacements
                                   replaced-keys)
           _ (println "replacements remaining:" key-replacements)
           
           cache (cache-node node raf cache)]
       (cond
        ; have more nodes in stack and keys to replace
        (and (seq stack) (seq key-replacements))
        (recur key-replacements stack raf cache)
        ; traversed entire stack, but still have keys to replace (bug)
        (seq key-replacements)
        (throw (ex-info (str "Keys " (keys key-replacements)
                             " missing from internal nodes")
                        {}))
        ; successfully replaced all keys
        :default cache))))

(defn- siblings
  "Returns a vector of the two siblings of node. If it doesn't have a sibling
  in one direction, nil will take its place."
  ([{:keys [offset]
     :as node}
    {:keys [key-ptrs
            last]
     :as parent}
    raf cache]
     (let [node-offset (:offset node)
           ; a seq of ptrs (nil p1 p2 ... last nil)
           ; nil's are added to both ends so that a valid pointer is
           ; always in the center when we partition by 3
           ptrs (lazy-cat [nil] (vals key-ptrs) [last nil])
           ; ((nil p1 p2) (p1 p2 p3) ... (pn last nil))
           triplets (partition 3 1 ptrs)
           ; gets the left and right pointers
           [prev-ptr _ next-ptr] (first (filter (fn [[prv mid nxt]]
                                                  (= mid node-offset))
                                                triplets))
           [prev-node cache] (get-node prev-ptr raf cache)
           [next-node cache] (get-node next-ptr raf cache)]
       [prev-node next-node])))

(defn- steal-prev
  "Steals a key-ptr from prev-leaf into leaf.
  Stolen key will become leaf's first key, so that key must be replaced
  in the internal nodes with the stolen key."
  ([leaf deleted-key prev-leaf stack raf cache]
     (let [; key currently at the front of leaf's key-ptrs
           first-key (-> leaf :key-ptrs first first)
           ; either the deleted key, or the first key has a copy in
           ; the internal nodes, and it's whichever comes
           ; lexicographically first
           internal-key (b-plus-tree.util/min-string deleted-key first-key)
           ; get the last key from the previous leaf
           [stolen-key stolen-ptr] (-> prev-leaf :key-ptrs last)
           ; delete the last key from the previous leaf
           prev-leaf (b-plus-tree.nodes/leaf-dissoc stolen-key prev-leaf)
           ; add the stolen key to the leaf
           leaf (b-plus-tree.nodes/leaf-assoc stolen-key stolen-ptr leaf)]
       ; replace the key in the internal nodes with the stolen key
       (replace-keys {internal-key stolen-key} stack raf
                     (cache-nodes [leaf prev-leaf] raf cache)))))

(defn- steal-next
  "Steals a key-ptr from next-leaf into leaf.
  Stolen key will be next-leaf's first key, so that key must be replaced
  in the internal nodes with the new first key.."
  ([leaf deleted-key next-leaf stack raf header cache]
     (let [; get the first key and ptr from the next leaf, and get
           ; the key that will become the first key so you can push it
           ; up through the internal nodes
           [[stolen-key stolen-ptr] [second-key _]]
           (->> next-leaf :key-ptrs (take 2))
           ; remove the stolen key/ptr from next-leaf
           next-leaf (b-plus-tree.nodes/leaf-dissoc stolen-key next-leaf)
           ; add the stolen key/ptr to leaf
           leaf (b-plus-tree.nodes/leaf-assoc stolen-key stolen-ptr leaf)
           ; push the key which succeeds stolen-key up into the
           ; internal nodes
           first-key (-> leaf :key-ptrs first first)
           ; need to replace stolen key with the key that follows it
           ; in all of the internal nodes
           replacement-keys {stolen-key second-key}
           ; if the key we deleted was the first key in the leaf, and
           ; this isn't the lowest sorted leaf in the tree, we need to
           ; replace that with its successor
           replacement-keys (if (and
                                 ; not the lowest leaf
                                 (pos? (:prev leaf))
                                 ; removed the lowest key
                                 (neg? (compare deleted-key first-key)))
                              (assoc replacement-keys
                                deleted-key first-key)
                              replacement-keys)]
       ; replace the necessary keys and return the cache
       (replace-keys replacement-keys stack raf
                     (cache-nodes [leaf next-leaf] raf cache)))))


(defn- merge-nodes
  "Recursively merges node."
  ([node]))

(defn- steal-merge
  "Attempts to steal from leaf's neighbors, and if it can't, merges."
  ([{:keys [prev next]
     :as leaf}
    key stack raf
    {:keys [order]
     :as header}
    cache]
     (let [parent (peek stack)
           [prev-leaf next-leaf] (siblings leaf parent raf cache)]
       (cond
        ; prev-leaf exists and can be stolen from
        (and prev-leaf (b-plus-tree.nodes/shrinkable? prev-leaf order))
        (steal-prev leaf key prev-leaf stack raf cache)
        ; next-leaf exists and can be stolen from
        (and next-leaf (b-plus-tree.nodes/shrinkable? next-leaf order))
        (steal-next leaf key next-leaf stack raf header cache)
        ; prev and next leaves cannot be stolen from, merge
        :default
        (do (println "prev:" (and prev-leaf (count (:key-ptrs prev-leaf))))
            (println "next:" (and next-leaf (count (:key-ptrs next-leaf))))
            (throw (new UnsupportedOperationException
                        "Merge not implemented.")))))))



(defn delete
  "Deletes key from the B+ Tree. Returns [header cache].
  All altered nodes in the cache have an [:altered? true] entry."
  ([key raf
    {size :count, root-ptr :root
     :keys [order key-size val-size page-size]
     :as header}
    & {:keys [cache]
       :or {cache {}}}]
     {:pre [(>= key-size (count key))]}
     (case size
       ; tree is empty, do nothing
       0 [header, cache]
       ; tree has one entry, just stop pointing to it
       1 [(assoc header
            :count 0
            :root  -1
            :free  page-size),
          {}]
       ; tree has multiple entries remaining
       (let [[root cache] (get-node root-ptr raf cache)
             [record stack cache] (find-stack key raf header
                                              :cache cache)
             [stack leaf] (b-plus-tree.seq/pop-stack stack)
             ; unpack leaf
             {:keys [type key-ptrs]} leaf]
;         (println "before:" leaf)
         (if (key-ptrs key)
           ; leaf contains key, remove it
           (let [header (assoc header
                          :count (dec size))
                 cache
                 (if (or
                      ; leaf is the root
                      (= type :root-leaf)
                      ; leaf is the left-most leaf
                      (= (:prev leaf) -1)
                      ; key is not the left-most key in the leaf
                      (not= key (-> leaf first first)))
                   ; key is not repeated in internal nodes
                   (let [leaf (b-plus-tree.nodes/leaf-dissoc key leaf)]
;                     (println "after:" leaf)
                     (if (b-plus-tree.nodes/underfull? leaf order)
                       ; leaf is underfull, steal or merge
                       (steal-merge leaf key stack raf header cache)
                       ; leaf contains enough elements, delete is finished
                       (cache-node leaf raf cache)))
                   ; key is repeated in internal nodes
                   (let [leaf (b-plus-tree.nodes/leaf-dissoc key leaf)
                         ; key which needs to replace the deleted key
                         ; in internal nodes
                         replacement-key (-> leaf :key-ptrs first first)]
                     (if (b-plus-tree.nodes/underfull? leaf order)
                       ; leaf is underfull, begin steal or merge
                       (steal-merge leaf key stack raf header cache)
                       ; leaf contains enough elements, simply replace key
                       ; with replacement-key in internal nodes
                       (replace-keys {key replacement-key} stack raf
                                     (cache-node leaf raf cache)))))]
             ; key was removed, remove changed header and cache
             [header, cache])

           ; key doesn't exist, return header and cache unchanged
           [header, cache])))))

(defn map-subset?
  "Returns true if the map m is a subset of the B+ Tree, else nil."
  ([m raf header
    & {:keys [cache]
       :or {cache {}}}]
     (every? identity (map (fn [[k v]] (= v (first (find-val k raf header
                                                            :cache cache))))
                           m))))

(defn map-equals?
  "Returns true if the map m is equal to the B+ Tree, else nil."
  ([m raf
    {size :count
     :as header}
    & {:keys [cache]
       :or {cache {}}}]
     (when (= size (count m))
       (map-subset? m raf header
                    :cache cache))))

(defn- lowest-leaf
  "Returns the lowest leaf in sorted order starting at node."
  ([node raf header cache]
     (if (b-plus-tree.nodes/leaf? node)
       node
       (let [next-ptr (-> node :key-ptrs first second)
             [next-node cache] (get-node next-ptr raf cache)]
         (recur next-node raf header cache)))))

(defn- highest-leaf
  "Returns the highest leaf in sorted order starting at node."
  ([node raf header cache]
     (if (b-plus-tree.nodes/leaf? node)
       node
       (let [next-ptr (:last node)
             [next-node cache] (get-node next-ptr raf cache)]
         (recur next-node raf header cache)))))

(defn- key-ptr->key-val
  "Given the key-ptr entry from a leaf node, replaces ptr with the value
  it points to."
  ([[key ptr] raf cache]
     (let [[record _] (get-node ptr raf cache)]
       [key (:data record)])))

(defn lowest-entry
  "Returns the lowest entry in the tree."
  ([raf header
    & {:keys [cache]
       :or {cache {}}}]
     (let [[root cache] (get-node (:root header) raf cache)]
       (-> (lowest-leaf root raf header cache)
           :key-ptrs
           first
           (key-ptr->key-val raf cache)))))

(defn highest-entry
  "Returns the highest entry in the tree."
  ([raf header
    & {:keys [cache]
       :or {cache {}}}]
     (let [[root cache] (get-node (:root header) raf cache)]
       (-> (highest-leaf root raf header cache)
           :key-ptrs
           last
           (key-ptr->key-val raf cache)))))

(def lowest-key
  ^{:tag String
    :doc "Returns the lowest key in the tree."
    :arglists '([raf header {cache :cache}])}
  (comp first lowest-entry))

(def highest-key
  ^{:tag String
    :doc "Returns the highest key in the tree."
    :arglists '([raf header {cache :cache}])}
  (comp first highest-entry))

(defn leaf-seq
  "Returns a seq of all the leaf nodes in the B+ Tree."
  ([raf header
    & {:keys [cache]
       :or {cache {}}}]
     (let [[root cache] (get-node (:root header) raf cache)
           first-node (lowest-leaf root raf header cache)
           step (fn step [prev-node]
                  (let [[next-node _] (get-node (:next prev-node) raf cache)]
                    (when next-node
                      (cons next-node (lazy-seq (step next-node))))))]
       (cons first-node (lazy-seq (step first-node))))))

(defn keyval-seq
  "Returns a seq of all the key-value pairs in the B+ Tree."
  ([raf header
    & {:keys [cache]
       :or {cache {}}}]
     (let [leaf-seq (leaf-seq raf header
                              :cache cache)
           key-ptrs (-> leaf-seq first :key-ptrs)
           leaf-seq (rest leaf-seq)
           step (fn step [key-ptrs leaf-seq]
                  (cond
                   ; still have key-ptrs from this node
                   (seq key-ptrs)
                   (let [entry (first key-ptrs)]
                     (cons (key-ptr->key-val entry raf cache)
                           (lazy-seq (step (rest key-ptrs)
                                           leaf-seq))))

                   ; still have leaves to go
                   (seq leaf-seq)
                   (step (-> leaf-seq first :key-ptrs)
                         (rest leaf-seq))))]
       (lazy-seq (step key-ptrs leaf-seq)))))

(defn print-leaf-keys
  "Prints the keys of all the leaf nodes in order, separating each node
  with a newline. Useful for debugging."
  ([raf header
    & {:keys [cache]
       :or {cache {}}}]
     (let [leaf-seq (leaf-seq raf header
                              :cache cache)]
       (doseq [leaf leaf-seq]
         (println (-> leaf :key-ptrs keys))))))

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
