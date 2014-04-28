(ns b-plus-tree.lookup-test
  "Tests for lookup functions"
  (:require [clojure.java.io :as io]
            [b-plus-tree core io nodes])
  (:use clojure.test))

(def root-node
  [{:type :root-nonleaf
    :free -1
    :page-size 100
    :key-ptrs (sorted-map "c" 100)
    :last 200
    :offset 0}])

(def internal-nodes
  [{:type :internal
    :key-ptrs (sorted-map "b" 300)
    :last 400
    :offset 100}
   {:type :internal
    :key-ptrs (sorted-map "d" 500,
                          "e" 600)
    :last 700
    :offset 200}])

(def leaf-nodes
  [{:type :leaf
    :key-ptrs (sorted-map "a" 800)
    :next 400
    :offset 300}
   {:type :leaf
    :key-ptrs (sorted-map "b" 900)
    :next 500
    :offset 400}
   {:type :leaf
    :key-ptrs (sorted-map "c" 1000)
    :next 600
    :offset 500}
   {:type :leaf
    :key-ptrs (sorted-map "d" 1100)
    :next 700
    :offset 600}
   {:type :leaf
    :key-ptrs (sorted-map "e" 1200,
                          "f" 1300)
    :next -1
    :offset 700}])

(def record-nodes
  [{:type :record
    :data "http://www.a.com"
    :offset 800}
   {:type :record
    :data "http://www.b.com"
    :offset 900}
   {:type :record
    :data "http://www.c.com"
    :offset 1000}
   {:type :record
    :data "http://www.d.com"
    :offset 1100}
   {:type :record
    :data "http://www.e.com"
    :offset 1200}
   {:type :record
    :data "http://www.f.com"
    :offset 1300}])

(def nodes
  (concat root-node internal-nodes leaf-nodes record-nodes))

(defn populate-file
  "Writes all nodes to file"
  ([nodes raf]
     (doall (map (fn [node] (b-plus-tree.io/write-node node raf))
                 nodes))))


(deftest find
  (testing "finding all records"
    (with-open [raf (new java.io.RandomAccessFile "/tmp/raf" "rwd")]
      (populate-file nodes raf)
      (doseq [[k v] {"a" "http://www.a.com",
                     "b" "http://www.b.com",
                     "c" "http://www.c.com",
                     "d" "http://www.d.com",
                     "e" "http://www.e.com",
                     "f" "http://www.f.com"}]
        (is (= (b-plus-tree.core/find k 100 raf) v))))
    (io/delete-file "/tmp/RAF" true)))

(deftest find-record-test
  (testing "should display all records that the leaves point to"
    (with-open [raf (new java.io.RandomAccessFile "/tmp/raf" "rwd")]
      (populate-file nodes raf)
      (doall (map (fn [node]
                    (doall (map (fn [[key ptr]]
                                  (is
                                   (=
                                    (b-plus-tree.io/read-node ptr raf)
                                    (b-plus-tree.core/find-record key
                                                                  node
                                                                  raf))))
                                (:key-ptrs node))))
                  leaf-nodes))
      (io/delete-file "/tmp/RAF" true))))

(comment
  (deftest retreive
    (testing "retreiving all records"
      (with-open [raf (new java.io.RandomAccessFile "/tmp/RAF" "rwd")]
        (doseq [[ptr node] nodes]
          (b-plus-tree.io/write-node node raf))
        (doseq [[key record-node] (map list
                                       ["a" "b" "c" "d" "e" "f"]
                                       (vals (sort record-nodes)))]
          (println "key:" key)
          (is (= (b-plus-tree.core/find key 100 raf)
                 (:data record-node)))))
      (io/delete-file "/tmp/RAF"))))

(comment
  (deftest slice
    (testing "slicing tree"
      (with-open [raf (new java.io.RandomAccessFile "/tmp/RAF" "rwd")]
        (doseq [[ptr node] nodes]
          (b-plus-tree.io/write-node node raf))
        (println "slice it up")
        (println "a:" (b-plus-tree.core/find-slice "a" raf))
        (println "b:e" (b-plus-tree.core/find-slice "b" "e" raf)))
      (io/delete-file "/tmp/RAF" true))))
