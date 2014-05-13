(ns b-plus-tree.lookup-test
  "Tests for lookup functions"
  (:require [clojure.java.io :as io]
            [b-plus-tree core io nodes])
  (:use clojure.test
        b-plus-tree.test-utils))

(def header-node
  {:count     6
   :free      1500
   :order     3
   :key-size  1
   :val-size  16
   :page-size 100
   :root      100})

(def root-node
  {:type :root-nonleaf
   :key-ptrs (sorted-map "c" 200)
   :last 300
   :offset 100})

(def internal-nodes
  [{:type :internal
    :key-ptrs (sorted-map "b" 400)
    :last 500
    :offset 200},
   {:type :internal
    :key-ptrs (sorted-map "d" 600,
                          "e" 700)
    :last 800
    :offset 300}])

(def leaf-nodes
  [{:type :leaf
    :key-ptrs (sorted-map "a" 900)
    :prev -1
    :next 500
    :offset 400},
   {:type :leaf
    :key-ptrs (sorted-map "b" 1000)
    :prev 400
    :next 600
    :offset 500},
   {:type :leaf
    :key-ptrs (sorted-map "c" 1100)
    :prev 500
    :next 700
    :offset 600},
   {:type :leaf
    :key-ptrs (sorted-map "d" 1200)
    :prev 600
    :next 800
    :offset 700},
   {:type :leaf
    :key-ptrs (sorted-map "e" 1300,
                          "f" 1400)
    :prev 700
    :next -1
    :offset 800}])

(def record-nodes
  [{:type :record
    :data "http://www.a.com"
    :offset 900},
   {:type :record
    :data "http://www.b.com"
    :offset 1000},
   {:type :record
    :data "http://www.c.com"
    :offset 1100},
   {:type :record
    :data "http://www.d.com"
    :offset 1200},
   {:type :record
    :data "http://www.e.com"
    :offset 1300},
   {:type :record
    :data "http://www.f.com"
    :offset 1400}])

(def nodes
  (concat [root-node] internal-nodes leaf-nodes record-nodes))

(defn populate-file
  "Writes all nodes to file"
  ([nodes raf]
     (doall (map (fn [node] (b-plus-tree.io/write-node node raf))
                 nodes))))


(deftest find-test
  (testing "finding all records"
    (with-open [raf (new java.io.RandomAccessFile "/tmp/RAF" "rwd")]
      (b-plus-tree.io/write-header header-node raf)
      (populate-file nodes raf)
      (let [header (b-plus-tree.io/read-header raf)]
        (doseq [[k v] {"a" "http://www.a.com",
                       "b" "http://www.b.com",
                       "c" "http://www.c.com",
                       "d" "http://www.d.com",
                       "e" "http://www.e.com",
                       "f" "http://www.f.com"}]
          (is (= (first (b-plus-tree.core/find-val k raf header)) v)))))
    (io/delete-file "/tmp/RAF" true)))

(with-private-fns [b-plus-tree.core [find-record]]
  (deftest find-record-test
    (testing "should display all records that the leaves point to"
      (with-open [raf (new java.io.RandomAccessFile "/tmp/RAF" "rwd")]
        (populate-file nodes raf)
        (doall (map (fn [node]
                      (doall (map (fn [[key ptr]]
                                    (is
                                     (=
                                      (b-plus-tree.io/read-node ptr raf)
                                      (first
                                       (find-record key
                                                    node
                                                    raf)))))
                                  (:key-ptrs node))))
                    leaf-nodes))
        (io/delete-file "/tmp/RAF" true)))))

(deftest lowest-highest-test
  (testing "find the lowest and highest leaves in a tree"
    (delete-file)
    (new-tree)
    (with-open [raf (get-raf)]
      (let [header (get-header raf)
            keyvals (numbered-strings 1000)
            [header cache]
            (b-plus-tree.core/insert-all keyvals raf header)
            lowest (-> keyvals first first)
            highest (-> keyvals last first)]
        (is (= lowest (b-plus-tree.core/lowest-key raf header
                                                   :cache cache)))
        (is (= highest (b-plus-tree.core/highest-key raf header
                                                     :cache cache)))))
    (delete-file)))

(deftest seq-test
  (testing "iterating through the tree's keyvals"
    (delete-file)
    (new-tree)
    (with-open [raf (get-raf)]
      (let [header         (get-header raf)
            keyvals        (numbered-strings 1000)
            [header cache] (b-plus-tree.core/insert-all keyvals raf header)]
        (doall (b-plus-tree.core/leaf-seq raf header
                                          :cache cache))
        (is (= (seq keyvals)
               (b-plus-tree.core/keyval-seq raf header
                                            :cache cache)))))
    (delete-file)))

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
