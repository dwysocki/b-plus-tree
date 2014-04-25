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
                  :key-ptrs (sorted-map "a" 1, "b" 2, "c" 3)
                  :offset 0}
                 {:type :root-nonleaf,
                  :next-free -1,
                  :key-ptrs (sorted-map "a" 5, "b" 4, "c" 6)
                  :last 1
                  :offset 500}
                 {:type :internal,
                  :key-ptrs (sorted-map "f" 3, "g" 10)
                  :last 17
                  :offset 1000}
                 {:type :leaf,
                  :key-ptrs (sorted-map "x" 10, "y" 15),
                  :next -1,
                  :offset 1500}
                 {:type :record,
                  :data "http://www.wikipedia.org",
                  :offset 2000}]]
      (io/delete-file fname true)
      (with-open [raf (new java.io.RandomAccessFile fname "rwd")]
        (doall (map #(b-plus-tree.io/write-node % raf) nodes))
        (doall (map #(is (= % (b-plus-tree.io/read-node (:offset %) raf)))
                    nodes)))
      (io/delete-file fname true))))
