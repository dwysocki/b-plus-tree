(ns b-plus-tree.remove-test
  (:require [clojure.java.io :as io]
            [b-plus-tree core io])
  (:use clojure.test
        b-plus-tree.test-utils))

(deftest no-merge-test
  (testing "removing from B+ Tree without any merges"
    (io/delete-file filename true)
    (b-plus-tree.io/new-tree filename order key-size val-size)
    (with-open [raf (new java.io.RandomAccessFile filename "rwd")]
      (let [header (b-plus-tree.io/read-header raf)
            keyvals (apply sorted-map (map str (-> order dec (* 2) range)))
            [header cache]
            (b-plus-tree.core/insert-all keyvals raf header)]))))
