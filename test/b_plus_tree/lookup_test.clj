(ns b-plus-tree.lookup-test
  "Tests for lookup functions"
  (:require [clojure.java.io :as io]
            [b-plus-tree core io nodes])
  (:use clojure.test))

(def root-node
  {0 {:type :root-nonleaf
      :nextfree -1
      :pagesize 100
      :keys ["c"]
      :children [100 200]}})

(def internal-nodes
  {100 {:type :internal
        :keys ["b"]
        :children [300 400]}
   200 {:type :internal
        :keys ["d" "e"]
        :children [500 600 700]}})

(def leaf-nodes
  {300 {:type :leaf
        :keys ["a"]
        :children [800]
        :nextleaf 400}
   400 {:type :leaf
        :keys ["b"]
        :children [900]
        :nextleaf 500}
   500 {:type :leaf
        :keys ["c"]
        :children [1000]
        :nextleaf 600}
   600 {:type :leaf
        :keys ["d"]
        :children [1100]
        :nextleaf 700}
   700 {:type :leaf
        :keys ["e" "f"]
        :children [1200 1300]
        :nextleaf -1}})

(def record-nodes
  {800  {:type :record
         :data "http://www.a.com"}
   900  {:type :record
         :data "http://www.b.com"}
   1000 {:type :record
         :data "http://www.c.com"}
   1100 {:type :record
         :data "http://www.d.com"}
   1200 {:type :record
         :data "http://www.e.com"}
   1300 {:type :record
         :data "http://www.f.com"}})

(def nodes
  (merge root-node internal-nodes leaf-nodes record-nodes))

(deftest retreive
  (testing "retreiving all records"
    (with-open [raf (new java.io.RandomAccessFile "/tmp/RAF" "rwd")]
      (doseq [[ptr node] nodes]
        (b-plus-tree.io/write-node node raf ptr))
      (doseq [[key record-node] (map list
                                     ["a" "b" "c" "d" "e" "f"]
                                     (vals (sort record-nodes)))]
        (println "key:" key)
        (is (= (b-plus-tree.core/find key raf)
               (:data record-node)))))
    (io/delete-file "/tmp/RAF")))

(deftest slice
  (testing "slicing tree"
    (with-open [raf (new java.io.RandomAccessFile "/tmp/RAF" "rwd")]
      (doseq [[ptr node] nodes]
        (b-plus-tree.io/write-node node raf ptr))
      (println "a:" (b-plus-tree.core/find-slice "a" raf))
      (println "b:e" (b-plus-tree.core/find-slice "b" "e" raf)))
    (io/delete-file "/tmp/RAF")))
