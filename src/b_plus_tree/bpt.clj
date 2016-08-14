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
  )

(defprotocol IDataNode
  (first-key [_]))
(def branching-factor 4)
(def b2! (-> (/ branching-factor 2) int))

(def gt? (comp pos? compare))

(def lt? (comp neg? compare))

(declare split )
(declare slice)
(declare find-insert-pos)
(declare find-marker-loc)
(declare conquer)
(declare validate-marker)

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
    (< (count markers) (dec branching-factor)))

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

          (let [node (nth (vec nodes) idx)]
            (if-not (data-node? node)
              (do
                (let [new-node (.insert node key val this)]
                  (try
                  (assoc this :nodes
                    (assoc nodes idx new-node))
                  (catch Throwable t
                    (do (reset! debug-list [this parent new-node]))
                    (log/error t)))
                    ))
              (if (and node (near-overflow? node))
                (let [[marker-candidate part-a part-b] (conquer (:nodes node) new-data-node)
                      [left right] (slice (vec nodes) idx)
                      right (rest right)
                      _ (validate-marker marker-candidate this parent)
                      new-markers (vec (apply sorted-set (conj (:markers this) marker-candidate)))
                      new-nodes (vec (concat left [part-a part-b] right ))]
                    ;; the next step is to check if the
                    ;; internode has maxout the available slots
                    ;;
                    (if (> (count new-markers) (dec branching-factor))
                      (let [[lmark-slice rmark-slice] (slice new-markers  b2!)
                            root-marker (first rmark-slice)
                            _ (validate-marker root-marker this parent)
                            position (find-insert-pos (prepare-nodes new-nodes) root-marker)
                            [subleft subright] (slice new-nodes position)]
                        (log/infof "Divide MARKERS!! %s vs %s " lmark-slice rmark-slice)
                        ;; before create the new sub nodes, we have to check
                        ;; if we can populate one of the marks to the up.
                        (when (and parent (data-node? parent))
                          (log/infof "Parent MARKERS!! %s - %s -> %s" (:markers parent) new-markers [lmark-slice rmark-slice])
                        )
                        (let [left-inter (->Node false lmark-slice subleft)
                              right-inter (->Node false (rest rmark-slice) subright)
                              root-inter (->Node false [root-marker] [left-inter right-inter])]
                        root-inter))
                      (do
                        (log/info "Parent has availability" marker-candidate)
                        (-> this
                          (assoc :markers new-markers)
                          (assoc :nodes new-nodes )))))
              (do
                (log/info "A Nice insert to" idx key val)
                (assoc this :nodes
                  (assoc nodes idx (.insert node  key val this))))))))

        (if-not (overflow? this)
          (let [position (find-insert-pos nodes key)
                _ (log/info "Item possible location " position )

                [left right] (slice nodes position)]
                (log/info "slice and dice" position)
            (assoc this :nodes (vec (concat left [new-data-node] right  ))))

          (let [[marker-candidate part-a part-b] (conquer nodes new-data-node)]
            (validate-marker marker-candidate this parent)
            (log/info "Splitting with candidate " marker-candidate (.hashCode this))

              (-> this
                (assoc :markers (conj (or markers []) marker-candidate))
                (assoc :nodes [part-a part-b])
                (assoc :data? false)
                )))))))

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
  [ (subvec (vec nodes) 0 loc)
    (subvec (vec nodes) loc)]
  )

(defn split [nodes]
  ; (log/info nodes)
  (let [
        part-a (subvec nodes 0 b2!)
        part-b (subvec nodes b2!)]
    (mapv (fn[x] (->Node true [] x))
      [part-a part-b]
  )))

(defn conquer
  [nodes new-data-node]
  (let [position (find-insert-pos nodes (:key new-data-node))
        [subleft subright] (slice nodes position)
        temp-node (vec (concat subleft [new-data-node] subright))
        [part-a part-b] (split temp-node)
        start-marker (first-key part-b )]
    [start-marker part-a part-b]))

(defn kickoff []
(def r2 (->Node true [] (mapv (fn [[x y]] (->Data x y))
            (seq (zipmap (range 0 50 10)(range 1 50 10))))))

(def r2 (-> r2 (.insert  50 0x4 nil)
 (.insert  60 0x4 nil)
 (.insert  70 0x4 nil)
 (.insert  80 0x4 nil)
 (.insert  90 0x4 nil)
 (.insert  100 0x4 nil)

; (def r2 (.insert r2 32 0x4 nil))
; (def r2 (.insert r2 "CCCDABAB" 0x5 nil))
; (def r2 (.insert r2 "CDEABABAB" 0x44 nil))
)))
(def history (atom []))
