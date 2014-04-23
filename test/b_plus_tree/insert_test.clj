(ns b-plus-tree.insert-test
  "Tests for insertion function."
  (:require [clojure.java.io :as io]
            [b-plus-tree core io])
  (:use clojure.test))

(def key-vals
  [["a" "alink"]
   ["b" "blink"]
   ["c" "clink"]
   ["d" "dlink"]
   ["ee" "eelink"]
   ["fff" "ffflink"]
   ["aabb" "aabblink"]
   ["zzyy" "zzyylink"]
   ["foo" "foolink"]
   ["bar" "barlink"]
   ["baz" "bazlink"]])

(deftest insert-simple-test
  (testing "simple insertion"
    (with-open [raf (new java.io.RandomAccessFile "/tmp/RAF" "rwd")]
      (doall (map (fn [[k v]]
                    (println "inserting" k)
                    (b-plus-tree.core/insert k v 4 100 raf))
                  key-vals))
      (doall (map (fn [[k v]]
                    (println "finding" k)
                    (is (= v (b-plus-tree.core/find k raf))))
                  key-vals)))
    (io/delete-file "/tmp/RAF")))
