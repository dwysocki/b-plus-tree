(ns b-plus-tree.insert-test
  "Tests for insertion function."
  (:require [clojure.java.io :as io]
            [b-plus-tree core io])
  (:use clojure.test))

(def order 32)
(def key-size 32)
(def val-size 32)

(deftest insert-single-test
  (testing "inserting single element"
    (io/delete-file "/tmp/RAF" true)
    (b-plus-tree.io/new-tree "/tmp/RAF" order key-size val-size)
    (with-open [raf (new java.io.RandomAccessFile "/tmp/RAF" "rwd")]
      (let [header (b-plus-tree.io/read-header raf)
            [header cache] (b-plus-tree.core/insert "foo" "bar" raf header)
            [cached-data cache] (b-plus-tree.core/find "foo" raf header
                                                       :cache cache)
            _ (b-plus-tree.io/write-cache cache raf)
            [uncached-data cache] (b-plus-tree.core/find "foo" raf header)]
        (is (= "bar" cached-data uncached-data))))
    (io/delete-file "/tmp/RAF" true)))

(deftest insert-simple-test
  (testing "simple insertion into root leaf"
    (io/delete-file "/tmp/RAF" true)
    (b-plus-tree.io/new-tree "/tmp/RAF" order key-size val-size)
    (with-open [raf (new java.io.RandomAccessFile "/tmp/RAF" "rwd")]
      (let [header (b-plus-tree.io/read-header raf)
            keyvals (apply sorted-map (map str (-> order (* 2) range)))
            [header cache]
            (b-plus-tree.core/insert-all keyvals raf header)]
        (b-plus-tree.io/write-cache cache raf)
        (loop [keyvals keyvals]
          (if-let [entry (first keyvals)]
            (let [[key val] entry
                  [cached-data cache] (b-plus-tree.core/find key raf header
                                                         :cache cache)
                  [uncached-data cache] (b-plus-tree.core/find key
                                                               raf header)]
              (is (= val cached-data uncached-data))
              (recur (next keyvals)))))))
    (io/delete-file "/tmp/RAF" true)))
