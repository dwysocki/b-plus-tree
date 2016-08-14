(ns b-plus-tree.bpt
  (:require [taoensso.timbre :as log]
            [b-plus-tree.util :refer [charset]]))
(use 'clojure.repl)

(try
    (require '[clojure.tools.namespace.repl :refer [refresh]])
    (catch Exception e nil))

(defprotocol INode
  (data-node? [_])
  (overflow? [_])
  (near-overflow? [_])
  (insert [_ key val parent])
  (has-key-places [_])
  (split-node [_ d parent])
  )

(defprotocol IDataNode
  (first-key [_]))
(def branching-factor 4)
(def b2! (-> (/ branching-factor 2) int))
(def history (atom []))
(def gt? (comp pos? compare))

(def lt? (comp neg? compare))

(declare split )
(declare slice)
(declare find-insert-pos)
(declare find-marker-loc)
(declare conquer)
(declare validate-marker)
(declare insert-to-subnode)
(declare slice-up)
(declare split-internals)
(def debug-list (atom []))

(defn prepare-nodes
  [new-nodes]
  (map
    (fn[x] (let [els (remove nil?
                    (mapv :key (:nodes x)))]
                    (when-not (empty? els)
                      {:key (apply max els)})
                    ))

    new-nodes)
  )

(defrecord Data[key val])
(defrecord Node [data? markers nodes ]
  clojure.lang.ISeq
  (first [this]
    (nth nodes 0))

  IDataNode
  (first-key [this]
    (-> (first nodes)
      (get :key)
    ))
  ; (seq [this] nodes)

  Object
  (toString [this]
    (format "%s#DataNode[%d]%s..." (.hashCode this) (count nodes) (mapv identity (take 4 nodes))))

  INode
  (data-node? [this] data? )

  (near-overflow? [this]
    (let [node-count (count nodes)]
      (log/infof "Near Overflowing? %d vs %d" node-count branching-factor)
      (>= (inc node-count) branching-factor)))

  (overflow? [this]
    (let [node-count (count nodes)]
      (log/infof "Overflowing %d vs %d" node-count branching-factor)
      (>= node-count branching-factor)))

  (has-key-places [this]
    (<= (count markers) (dec branching-factor)))

  (split-node [this new-data-node parent]
    (log/info "Splitting node")
    (let [[marker-candidate part-a part-b] (conquer (:nodes this) new-data-node)]
      (validate-marker marker-candidate this parent)
      (log/info "Splitting with candidate " marker-candidate (.hashCode this))

        (-> this
          (assoc :markers (conj (or (:markers this) []) marker-candidate))
          (assoc :nodes [part-a part-b])
          (assoc :data? false)
          )))

  (insert [this key val parent]
    (log/infof "Inserting|%s->%s" key val)
    (let [new-data-node (->Data key val)]
      (if-not (data-node? this)
        (let [marker-count (count markers)
              [index k] (find-marker-loc markers key)
              ;; k nil indicates we iterated the whole array.
              ;; definitely feels there is one (or more) bug(s) here..
              idx (if (nil? k) (dec (count nodes)) index) ]
          (log/infof "Node position:[%d-%d] BigBro:%s Me:%s %s" index idx k key markers)

          (let [subnode (nth (vec nodes) idx)]
            (if-not (data-node? subnode)
              (let [new-node (.insert subnode key val this)]
                  (try (assoc this :nodes
                        (assoc nodes idx new-node))
                  (catch Throwable t
                    (do (reset! debug-list [this parent new-node]))
                    (log/error t))))

              (let [this-node (let [[node' this-node] (insert-to-subnode subnode idx this parent new-data-node key val)]
                                (log/info "Inserted subnode")
                                (let [this' (if-not (nil? node')
                                              (assoc this :nodes
                                                (assoc nodes idx node'))
                                              this)]
                                  (if-not (nil? this-node)
                                    (do
                                      (log/info "Updated node")
                                      (-> this'
                                          (assoc :markers (:markers this-node))
                                          (assoc :nodes (:nodes this-node))))
                                     this')))]
                  (if (has-key-places this-node)
                    (do (log/info "OK...") this-node)
                    (split-internals this-node parent))
                  ))))

        (if-not (overflow? this)
          (let [position (find-insert-pos nodes key)
                _ (log/info "Item possible location " position )

                [left right] (slice nodes position)]
                (log/info "slice and dice" position)
            (assoc this :nodes (vec (concat left [new-data-node] right  ))))

          (split-node this new-data-node parent)
                )))))

(defn slice-node-into-half
  "Slice the complete node into half."
  [this]
  (let [[lmark-slice rmark-slice] (slice (:markers this)  b2!)
        root-marker (first rmark-slice)
        position (find-insert-pos (prepare-nodes (:nodes this)) root-marker)
        [subleft subright] (slice (:nodes this) position)]

    [root-marker
      subleft subright
      lmark-slice rmark-slice]))

(defn split-internals
  "Split the node; depending on if parent node exists
  populate a marker to the parent, and re-arrange the internal nodes.
  Otherwise creates two internal nodes and attaches to the node at hand"
  [this parent]
  (let [[root-marker subleft subright lmark-slice rmark-slice] (slice-node-into-half this)]
    (if (nil? parent)
      (let [left-inter (->Node false lmark-slice subleft)
            right-inter (->Node false (rest rmark-slice) subright)
            root-inter (->Node false [root-marker] [left-inter right-inter])]
        (log/info lmark-slice rmark-slice )
        root-inter)
      (let [left-inter (->Node false lmark-slice subleft)
            right-inter (->Node false (rest rmark-slice) subright)]
        ;; now the parent is not
        (log/info "Parent is not nil. We dont need to create internal nodes yet.")
        (log/info root-marker lmark-slice rmark-slice)
        ;; the following placement does not take into account
        ;; whether the populated `marker` is greater or smaller than the existing
        ;; keys
        (swap! debug-list conj (-> parent
            (assoc :markers (concat [root-marker] (:markers parent)))
            (assoc :nodes   (concat [left-inter right-inter] (rest (:nodes parent) )))
          ))
        left-inter)

  )))

(defn insert-to-subnode [node idx this parent new-data-node key val]
  (log/infof "INSERT SUBNODE #%d" idx )
  (if (and node (overflow? node))
    (let [[marker-candidate part-a part-b] (conquer (:nodes node) new-data-node)
          [left right] (slice (vec (:nodes this)) idx)
          right (rest right)
          _ (validate-marker marker-candidate this parent)
          new-markers (vec (apply sorted-set (conj (:markers this) marker-candidate)))
          new-nodes (vec (concat left [part-a part-b] right ))]
        (log/info "Parent has availability" marker-candidate)
        (swap! history conj { :markers new-markers
          :nodes new-nodes})
        [nil {:markers new-markers
              :nodes new-nodes}])
  (do
    (log/info "A Nice insert to" idx key val)
     (let [result (.insert node key val this)
          _ (log/info "Resulted in smth" (count result) (type result))]
        (swap! history conj result)
        (if (vector? result)
          result
          [result nil])))))

(defn validate-marker
  [marker this parent]
  (when (nil? marker)
      (reset! debug-list [this parent])
      (throw (ex-info "Marker is nil!" {}))))

(defn find-marker-loc
  "Returns the elements whose key
  value is greater than this key"
  [markers key]
  (let [marker-count (count markers)]
    (loop [i 0]
      (log/infof "Compare#%d %s" i key )
      (if  (lt? key (nth markers i) )
                  [i (nth markers i)]
                (if (< (inc i) marker-count)
                  (recur (inc i))
                  [(inc i) nil])))))

(defn find-insert-pos
  "Returns the elements whose key
  value is greater than this key.
  See the similarity with find-marker-loc.
  Because it's a duplicate code!!"
  [nodes key]
  (let [magic (zipmap (range (count nodes)) nodes )
      position (->> (seq magic)
           (mapv (fn [[idx val]]
                    (log/infof "Search %s vs %s" (:key val) key)
                  (when (gt? (:key val) key )
                    idx)))
            (remove nil? ))]
      (if (empty? position)
          (count nodes)
          (first position)  )))

(defn slice [nodes loc]
  (log/info "Slice nodes" loc)
  [ (subvec (vec nodes) 0 loc)
    (subvec (vec nodes) loc)]
  )

(defn split [nodes]
  (log/info "Split nodes")
  (let [
        part-a (subvec nodes 0 b2!)
        part-b (subvec nodes b2!)]
    (mapv (fn[x] (->Node true [] x))
      [part-a part-b]
  )))

(defn conquer
  [nodes new-data-node]
  (log/info "Conquer for " (:key new-data-node) (count nodes) )
  (let [position (find-insert-pos nodes (:key new-data-node))
        [subleft subright] (slice nodes position)
        temp-node (vec (concat subleft [new-data-node] subright))
        [part-a part-b] (split temp-node)
        start-marker (first-key part-b )]
    [start-marker part-a part-b]))

(defn node-iter
  [root writer]
  (loop [i 0
         nodes [root]]
     (when-not (empty? nodes)
       (let [subnode-writer
               (fn [p]
                 (when (contains? p :data?)
                  (.append writer (format "\"%s\" [label = \"%s\"];\n" (.hashCode p) (clojure.string/join "-"
                    (if (empty? (:markers p))
                      [(.hashCode p)]
                      (:markers p))) )))
                   (mapv (fn [n]
                     (.append writer (format "\"%s\"->\"%s\"; \n" (.hashCode p) (if (contains? n :data?)
                                                                                  (.hashCode n)
                                                                                  (:key n))))
                     n)
                     (:nodes p)))]

            (recur (inc i)
                    (mapcat subnode-writer nodes))
         ))))

(defn snap [tree f]
  (let [dot (StringBuilder. "digraph rendering { ")]
    (node-iter tree dot)
    (.append dot "}")
    (spit f dot)
  ))


(defn kickoff []
(def r2 (->Node true [] (mapv (fn [[x y]] (->Data x y))
            (seq (zipmap (range 0 50 10)(range 1 50 10))))))

(def r2 (-> r2 (.insert  50 0x4 nil)
 (.insert  60 0x4 nil)
 (.insert  70 0x4 nil)
 (.insert  80 0x4 nil)
 (.insert  57 0x4 nil)
 (.insert  58 0x4 nil)
 (.insert  59 0x4 nil)
  (.insert  51 0x4 nil)
  (.insert  52 0x4 nil)
  (.insert  53 0x4 nil)
  (.insert  54 0x4 nil)
  (.insert  90 0x4 nil)
  (.insert  100 0x4 nil)

; (def r2 (.insert r2 32 0x4 nil))
; (def r2 (.insert r2 "CCCDABAB" 0x5 nil))
; (def r2 (.insert r2 "CDEABABAB" 0x44 nil))
)))
