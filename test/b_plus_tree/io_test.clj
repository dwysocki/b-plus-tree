(ns b-plus-tree.io-test
  "Tests for I/O functions."
  (:require [clojure.java.io :as io]
            [b-plus-tree io nodes])
  (:use clojure.test))

(deftest read-write
  (testing "basic read/write operations for all node types"
    (let [fname "/tmp/RAF"
          nodes [{:type :root-leaf,
                  :next-free -1,
                  :keys ["a" "b" "c"],
                  :children [1 2 3 4],
                  :offset 0}
                 {:type :root-nonleaf,
                  :next-free -1,
                  :keys ["a" "b" "c"],
                  :children [5 4 6 1],
                  :offset 100}
                 {:type :internal,
                  :keys ["f" "g"],
                  :children [3 10 17],
                  :offset 200}
                 {:type :leaf,
                  :keys ["x" "y"],
                  :children [10 15],
                  :next-leaf 16,
                  :offset 300}
                 {:type :record,
                  :data "http://www.wikipedia.org",
                  :offset 400}]]
      (io/delete-file fname)
      (with-open [raf (new java.io.RandomAccessFile fname "rwd")]
        (doall (map #(b-plus-tree.io/write-node % raf) nodes))
        (doall (map #(is (= % (b-plus-tree.io/read-node (:offset %) raf)))
                    nodes)))
      (io/delete-file fname))))
