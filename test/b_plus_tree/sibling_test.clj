(ns b-plus-tree.sibling-test
  (:require [clojure.java.io :as io]
            [b-plus-tree core io nodes])
  (:use clojure.test
        b-plus-tree.test-utils))

(def cache
  {1 {:type :root-nonleaf
      :key-ptrs (sorted-map "d" 2
                            "g" 3)
      :last 4
      :offset 1}
   2 {:type :internal
      :key-ptrs (sorted-map "b" 5
                            "c" 6)
      :last 7
      :offset 2}
   3 {:type :internal
      :key-ptrs (sorted-map "e" 8
                            "f" 9)
      :last 10
      :offset 3}
   4 {:type :internal
      :key-ptrs (sorted-map "h" 11
                            "i" 12)
      :last 13
      :offset 4}
   5 {:type :leaf
      :key-ptrs (sorted-map "a" 100)
      :prev -1
      :next 6
      :offset 5}
   6 {:type :leaf
      :key-ptrs (sorted-map "b" 101)
      :prev 5
      :next 7
      :offset 6}
   7 {:type :leaf
      :key-ptrs (sorted-map "c" 102)
      :prev 6
      :next 8
      :offset 7}
   8 {:type :leaf
      :key-ptrs (sorted-map "d" 103)
      :prev 7
      :next 9
      :offset 8}
   9 {:type :leaf
      :key-ptrs (sorted-map "e" 104)
      :prev 8
      :next 10
      :offset 9}
   10 {:type :leaf
       :key-ptrs (sorted-map "f" 105)
       :prev 9
       :next 11
       :offset 10}
   11 {:type :leaf
       :key-ptrs (sorted-map "g" 106)
       :prev 10
       :next 12
       :offset 11}
   12 {:type :leaf
       :key-ptrs (sorted-map "h" 107)
       :prev 11
       :next 13
       :offset 12}
   13 {:type :leaf
       :key-ptrs (sorted-map "i" 108)
       :prev 12
       :next -1
       :offset 13}})

(with-private-fns [b-plus-tree.core [siblings]]
  (deftest sibling-test
    (testing "testing siblings"
      (is (= [nil (cache 3)]
             (siblings (cache 2)
                       (cache 1)
                       nil cache)))
      (is (= [(cache 2) (cache 4)]
             (siblings (cache 3)
                       (cache 1)
                       nil cache)))
      (is (= [(cache 3) nil]
             (siblings (cache 4)
                       (cache 1)
                       nil cache))))))
