(ns b-plus-tree.io-test
  "Tests for I/O functions."
  (:require [clojure.java.io :as io]
            [b-plus-tree.io :as tree-io]
            [b-plus-tree.nodes :as tree-nodes])
  (:use clojure.test))

(deftest read-write
  (testing "basic read/write operations for all node types"
    (let [fname "/tmp/RAF"
          raf (new java.io.RandomAccessFile "/tmp/RAF" "rwd")
          nodes [{:type :root-leaf,
                  :nextfree -1,
                  :keys ["a" "b" "c"],
                  :children [1 2 3 4]}
                 {:type :root-nonleaf,
                  :nextfree -1,
                  :keys ["a" "b" "c"],
                  :children [5 4 6 1]}
                 {:type :internal,
                  :keys ["f" "g"]
                  :children [3 10 17]}
                 {:type :leaf,
                  :keys ["x" "y"]
                  :children [10 15]
                  :nextleaf 16}
                 {:type :record
                  :data "http://www.wikipedia.org"}]]
      (with-open [raf (new java.io.RandomAccessFile fname "rwd")]
        (loop [nodes nodes
               nextptr 0]
          (when (not-empty nodes)
            (recur (rest nodes)
                   (tree-io/write-node (first nodes) raf nextptr))))
        (loop [nodes nodes
               nextptr 0]
          (when-let [expected-node (first nodes)]
            (is (= expected-node
                   (tree-io/read-node raf nextptr)))
            (recur (rest nodes)
                   (.getFilePointer raf)))))
      (io/delete-file fname))))
