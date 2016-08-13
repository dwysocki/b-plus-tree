(ns user
  (:use rhizome.viz)
  (:require
    [taoensso.timbre :as log]
    [b-plus-tree.core :as core]
    [b-plus-tree.io :as b.io]
    [b-plus-tree.heap :as heap]
    [clojure.string :as s]
    [clojure.java.io :as io]
    [b-plus-tree.util :refer [charset]]))



(try
  (require '[clojure.tools.namespace.repl :refer [refresh]])
  (catch Exception e nil))

(defonce f (str "/tmp/btreefile" (System/nanoTime)))
(def order    32)
(def key-size 32)
(def val-size 32)

(declare insert-data)

(defn kickoff [reset]
  (when reset
    (b-plus-tree.io/new-tree f order key-size val-size))

  (with-open [raf (java.io.RandomAccessFile. f "rwd")]
    (let [header (b.io/read-header raf) i 1234]
      (let [[header' cache] (core/insert (str "foo" i) (str i "bar") raf header )]

        (b-plus-tree.io/write-cache cache raf)
        (b-plus-tree.io/write-header header' raf)
        [header' cache]))))


(def words (take 100000 (s/split (slurp "/usr/share/dict/words") #"\n")))
(def word-list (zipmap words (map (fn[x] (s/replace (->> x reverse (s/join "")) #"\'" ""))  words)))

(defn word-test [raf & {:keys [max-keys] :or {max-keys 1000} }]


  (let [bulk-tx (core/insert-all word-list raf (b.io/read-header raf) ) ]
    (b.io/write-header (first bulk-tx) raf)
    (b.io/write-cache (last bulk-tx) raf)
    ))



(defn heap-test
  []
  (let [heapobj (heap/init "00000000" 123)]
    (mapv (fn[[x y]] (.insert heapobj x y)) (take 10 (seq word-list)))
    heapobj
    ))


 (def builder (StringBuilder. "digraph pict { \n size=\"12,12\"; node [color=lightblue2, style=filled]; \n"))
