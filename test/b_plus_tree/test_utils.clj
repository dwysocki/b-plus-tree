(ns b-plus-tree.test-utils
  "Functions needed for unit tests"
  (:require [clojure.java.io :as io]))

(def ^:dynamic order 32)
(def key-size 32)
(def val-size 32)
(def filename "/tmp/RAF")

(defn new-tree
  "Make a new tree with default parameters."
  ([] (b-plus-tree.io/new-tree filename order key-size val-size)))

(defn get-raf
  "Return the RAF to use."
  ([] (new java.io.RandomAccessFile filename "rwd")))

(defn get-header
  "Return the header from the raf"
  ([raf] (b-plus-tree.io/read-header raf)))

(defn delete-file
  ([] (io/delete-file filename true)))

(defn numbered-strings
  "Return a sorted-map of numbered strings {1 2, 3 4, ...}"
  ([n] (apply sorted-map (map str (range (* 2 n))))))

(defmacro with-private-fns
  "Refers private fns from ns and runs tests in context.
  Author: Chris Houser <https://github.com/Chouser>"
  ([[ns fns] & tests]
     `(let ~(reduce #(conj %1 %2 `(ns-resolve '~ns '~%2)) [] fns)
        ~@tests)))
