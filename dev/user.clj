(ns user
  (:require
    [b-plus-tree.core :as core]
    [b-plus-tree.io :as b.io]
    [clojure.string :as s]
    [clojure.java.io :as io]))

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
        [header' cache]
  ))
  ))


; (def header (b.io/read-header raf))

(defn word-test [raf & {:keys [max-keys] :or {max-keys 1000} }]
  (let [words (take max-keys (s/split (slurp "/usr/share/dict/words") #"\n"))
        word-list (zipmap words (map (fn[x] (->> x reverse (s/join "")))  words))]

  (let [bulk-tx (core/insert-all word-list raf (b.io/read-header raf) ) ]
    (b.io/write-header (first bulk-tx) raf)
    (b.io/write-cache (last bulk-tx) raf)
  )))
