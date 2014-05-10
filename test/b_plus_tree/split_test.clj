(ns b-plus-tree.split-test
  "Tests for splitting nodes."
  (:require [b-plus-tree core]
            [b-plus-tree.util :refer [dbg]])
  (:use clojure.test))

(def order 4)
(def page-size 100)

(deftest split-root-leaf-test
  (testing "splitting root-leaf node"
    (let [header {:free      (* 6 page-size)
                  :page-size page-size}
          root-offset page-size
          left-offset (* 6 page-size)
          right-offset (* 7 page-size)
          root-leaf {:type :root-leaf
                     :key-ptrs (sorted-map "A" (* 2 page-size),
                                           "B" (* 3 page-size),
                                           "C" (* 4 page-size),
                                           "D" (* 5 page-size))
                     :offset root-offset}
          left-node {:type :leaf
                     :key-ptrs (sorted-map "A" (* 2 page-size)
                                           "B" (* 3 page-size))
                     :next right-offset
                     :prev -1
                     :offset left-offset
                     :altered? true}
          right-node {:type :leaf
                      :key-ptrs (sorted-map "C" (* 4 page-size)
                                            "D" (* 5 page-size))
                      :next -1
                      :prev left-offset
                      :offset right-offset
                      :altered? true}
          new-root {:type :root-nonleaf
                    :key-ptrs (sorted-map "C" left-offset)
                    :last right-offset
                    :offset root-offset
                    :altered? true}

          [header cache]
          (b-plus-tree.core/split-root-leaf root-leaf nil header)]
      (is (= left-node (cache left-offset)))
      (is (= right-node (cache right-offset)))
      (is (= new-root (cache root-offset))))))

(deftest split-root-nonleaf-test
  (testing "splitting root-leaf node"
    (let [header {:free      (* 7 page-size)
                  :page-size page-size}
          root-offset page-size
          left-offset (* 7 page-size)
          right-offset (* 8 page-size)
          orig-root {:type :root-nonleaf
                     :key-ptrs (sorted-map "A" (* 2 page-size),
                                           "B" (* 3 page-size),
                                           "C" (* 4 page-size),
                                           "D" (* 5 page-size))
                     :last (* 6 page-size)
                     :offset root-offset}
          left-node {:type :internal
                     :key-ptrs (sorted-map "A" (* 2 page-size))
                     :last (* 3 page-size)
                     :offset left-offset
                     :altered? true}
          right-node {:type :internal
                      :key-ptrs (sorted-map "C" (* 4 page-size)
                                            "D" (* 5 page-size))
                      :last (* 6 page-size)
                      :offset right-offset
                      :altered? true}
          new-root {:type :root-nonleaf
                    :key-ptrs (sorted-map "B" left-offset)
                    :last right-offset
                    :offset root-offset
                    :altered? true}

          [header cache]
          (b-plus-tree.core/split-root-nonleaf orig-root nil header)]
      (is (= left-node (cache left-offset)))
      (is (= right-node (cache right-offset)))
      (is (= new-root (cache root-offset))))))
