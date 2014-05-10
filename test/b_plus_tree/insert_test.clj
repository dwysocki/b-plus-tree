(ns b-plus-tree.insert-test
  "Tests for insertion function."
  (:require [clojure.java.io :as io]
            [b-plus-tree core io])
  (:use clojure.test))

(def order    32)
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
            keyvals1 (apply sorted-map (map str (-> order (* 2) range)))
            keyvals2 (reduce (fn [m [k v]] (assoc m k (str v 2)))
                             (sorted-map)
                             keyvals1)
            [header cache]
            (b-plus-tree.core/insert-all keyvals1 raf header)]
        ; confirming that all entries can be found in the cache
        (is (b-plus-tree.core/map-equals? keyvals1 raf header
                                         :cache cache))
        ; writing cache to disc
        (b-plus-tree.io/write-cache cache raf)
        ; confirming that all entries can be found on disc
        (is (b-plus-tree.core/map-equals? keyvals1 raf header))
        ; overwriting all entries, and running the same checks
        (let [[header cache]
              (b-plus-tree.core/insert-all keyvals2 raf header)]
          (is (b-plus-tree.core/map-equals? keyvals2 raf header
                                           :cache cache))
          (b-plus-tree.io/write-cache cache raf)
          (is (b-plus-tree.core/map-equals? keyvals2 raf header)))))
    (io/delete-file "/tmp/RAF" true)))
