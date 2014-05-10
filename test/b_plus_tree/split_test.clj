(ns b-plus-tree.split-test
  "Tests for splitting nodes."
  (:require [b-plus-tree core]
            [b-plus-tree.util :refer [dbg]])
  (:use clojure.test
        b-plus-tree.test-utils))

(def order 4)
(def page-size 100)

(with-private-fns [b-plus-tree.core [split-root-leaf]]
  (deftest split-root-leaf-test-even
    (testing "splitting even root-leaf node"
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
            (split-root-leaf root-leaf nil header)]
        (is (= left-node (cache left-offset)))
        (is (= right-node (cache right-offset)))
        (is (= new-root (cache root-offset)))))))

(with-private-fns [b-plus-tree.core [split-root-leaf]]
  (deftest split-root-leaf-test-odd
    (testing "splitting odd root-leaf node"
      (let [header {:free      (* 7 page-size)
                    :page-size page-size}
            root-offset page-size
            left-offset (* 7 page-size)
            right-offset (* 8 page-size)
            root-leaf {:type :root-leaf
                       :key-ptrs (sorted-map "A" (* 2 page-size)
                                             "B" (* 3 page-size)
                                             "C" (* 4 page-size)
                                             "D" (* 5 page-size)
                                             "E" (* 6 page-size))
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
                                              "D" (* 5 page-size)
                                              "E" (* 6 page-size))
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
            (split-root-leaf root-leaf nil header)]
        (is (= left-node (cache left-offset)))
        (is (= right-node (cache right-offset)))
        (is (= new-root (cache root-offset)))))))

(with-private-fns [b-plus-tree.core [split-root-nonleaf]]
  (deftest split-root-nonleaf-test-even
    (testing "splitting even root-nonleaf node"
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
            (split-root-nonleaf orig-root nil header)]
        (is (= left-node (cache left-offset)))
        (is (= right-node (cache right-offset)))
        (is (= new-root (cache root-offset)))))))

(with-private-fns [b-plus-tree.core [split-root-nonleaf]]
  (deftest split-root-nonleaf-test-odd
    (testing "splitting odd root-nonleaf node"
      (let [header {:free      (* 8 page-size)
                    :page-size page-size}
            root-offset page-size
            left-offset (* 8 page-size)
            right-offset (* 9 page-size)
            orig-root {:type :root-nonleaf
                       :key-ptrs (sorted-map "A" (* 2 page-size)
                                             "B" (* 3 page-size)
                                             "C" (* 4 page-size)
                                             "D" (* 5 page-size)
                                             "E" (* 6 page-size))
                       :last (* 7 page-size)
                       :offset root-offset}
            left-node {:type :internal
                       :key-ptrs (sorted-map "A" (* 2 page-size)
                                             "B" (* 3 page-size))
                       :last (* 4 page-size)
                       :offset left-offset
                       :altered? true}
            right-node {:type :internal
                        :key-ptrs (sorted-map "D" (* 5 page-size)
                                              "E" (* 6 page-size))
                        :last (* 7 page-size)
                        :offset right-offset
                        :altered? true}
            new-root {:type :root-nonleaf
                      :key-ptrs (sorted-map "C" left-offset)
                      :last right-offset
                      :offset root-offset
                      :altered? true}

            [header cache]
            (split-root-nonleaf orig-root nil header)]
        (is (= left-node (cache left-offset)))
        (is (= right-node (cache right-offset)))
        (is (= new-root (cache root-offset)))))))

(with-private-fns [b-plus-tree.core [split-internal]]
  (deftest split-internal-test-even
    (testing "spliting even internal node"
      (let [header {:free      (* 7 page-size)
                    :page-size page-size}
            orig-offset page-size
            right-offset (* 7 page-size)
            orig-node  {:type :internal
                        :key-ptrs (sorted-map "A" (* 2 page-size)
                                              "B" (* 3 page-size)
                                              "C" (* 4 page-size)
                                              "D" (* 5 page-size))
                        :last (* 6 page-size)
                        :offset orig-offset},
            left-node  {:type :internal
                        :key-ptrs (sorted-map "A" (* 2 page-size))
                        :last (* 3 page-size)
                        :offset orig-offset
                        :altered? true},
            right-node {:type :internal
                        :key-ptrs (sorted-map "C" (* 4 page-size)
                                              "D" (* 5 page-size))
                        :last (* 6 page-size)
                        :offset right-offset
                        :altered? true},
            
            [header cache [raised-key raised-offset]]
            (split-internal orig-node nil header)]
        (is (= left-node (cache orig-offset)))
        (is (= right-node (cache right-offset)))
        (is (= raised-key "B"))
        (is (= raised-offset right-offset))))))

(with-private-fns [b-plus-tree.core [split-internal]]
  (deftest split-internal-test-odd
    (testing "spliting odd internal node"
      (let [header {:free      (* 8 page-size)
                    :page-size page-size}
            orig-offset page-size
            right-offset (* 8 page-size)
            orig-node  {:type :internal
                        :key-ptrs (sorted-map "A" (* 2 page-size)
                                              "B" (* 3 page-size)
                                              "C" (* 4 page-size)
                                              "D" (* 5 page-size)
                                              "E" (* 6 page-size))
                        :last (* 7 page-size)
                        :offset orig-offset},
            left-node  {:type :internal
                        :key-ptrs (sorted-map "A" (* 2 page-size)
                                              "B" (* 3 page-size))
                        :last (* 4 page-size)
                        :offset orig-offset
                        :altered? true},
            right-node {:type :internal
                        :key-ptrs (sorted-map "D" (* 5 page-size)
                                              "E" (* 6 page-size))
                        :last (* 7 page-size)
                        :offset right-offset
                        :altered? true},
            
            [header cache [raised-key raised-offset]]
            (split-internal orig-node nil header)]
        (is (= left-node (cache orig-offset)))
        (is (= right-node (cache right-offset)))
        (is (= raised-key "C"))
        (is (= raised-offset right-offset))))))

(with-private-fns [b-plus-tree.core [split-leaf]]
  (deftest split-leaf-test-even
    (testing "splitting even leaf node"
      (let [header {:free      (* 6 page-size)
                    :page-size page-size},
            orig-offset page-size,
            right-offset (* 6 page-size),
            orig-leaf {:type :leaf
                       :key-ptrs (sorted-map "A" (* 2 page-size)
                                             "B" (* 3 page-size)
                                             "C" (* 4 page-size)
                                             "D" (* 5 page-size))
                       :prev 1000
                       :next 2000
                       :offset orig-offset},
            left-node {:type :leaf
                       :key-ptrs (sorted-map "A" (* 2 page-size)
                                             "B" (* 3 page-size))
                       :prev 1000
                       :next right-offset
                       :offset orig-offset
                       :altered? true},
            right-node {:type :leaf
                        :key-ptrs (sorted-map "C" (* 4 page-size)
                                              "D" (* 5 page-size))
                        :prev orig-offset
                        :next 2000
                        :offset right-offset
                        :altered? true},

            [header cache [raised-key raised-offset]]
            (split-leaf orig-leaf nil header)]
        (is (= raised-key "C"))
        (is (= raised-offset right-offset))
        (is (= left-node (cache orig-offset)))
        (is (= right-node (cache right-offset)))))))

(with-private-fns [b-plus-tree.core [split-leaf]]
  (deftest split-leaf-test-odd
    (testing "splitting odd leaf node"
      (let [header {:free      (* 7 page-size)
                    :page-size page-size},
            orig-offset page-size,
            right-offset (* 7 page-size),
            orig-leaf {:type :leaf
                       :key-ptrs (sorted-map "A" (* 2 page-size)
                                             "B" (* 3 page-size)
                                             "C" (* 4 page-size)
                                             "D" (* 5 page-size)
                                             "E" (* 6 page-size))
                       :prev 1000
                       :next 2000
                       :offset orig-offset},
            left-node {:type :leaf
                       :key-ptrs (sorted-map "A" (* 2 page-size)
                                             "B" (* 3 page-size))
                       :prev 1000
                       :next right-offset
                       :offset orig-offset
                       :altered? true},
            right-node {:type :leaf
                        :key-ptrs (sorted-map "C" (* 4 page-size)
                                              "D" (* 5 page-size)
                                              "E" (* 6 page-size))
                        :prev orig-offset
                        :next 2000
                        :offset right-offset
                        :altered? true},

            [header cache [raised-key raised-offset]]
            (split-leaf orig-leaf nil header)]
        (is (= raised-key "C"))
        (is (= raised-offset right-offset))
        (is (= left-node (cache orig-offset)))
        (is (= right-node (cache right-offset)))))))
