(ns user
  (:require
    [b-plus-tree.core :as core]
    [b-plus-tree.io :as b.io]
    [clojure.java.io :as io]))

(try
  (require '[clojure.tools.namespace.repl :refer [refresh]])
  (catch Exception e nil))

(def f "/tmp/btreefile")
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
        header'
  ))
  ))


; (def header (b.io/read-header raf))

(defn insert-data
  [raf header]
    (loop [i 0
           header header
           cache nil]
      (if (> i 1000)
        [header cache]
        (let [rez (core/insert (str "foo" i) (str i "bar") raf header )]
          (recur (inc i) (first rez) (last rez) ))))

          )
