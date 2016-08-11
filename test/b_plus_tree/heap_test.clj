(ns b-plus-tree.heap-test
  (:require [clojure.string :as s]
            [b-plus-tree.heap :as heap])
  (:use clojure.test)
  )



(def words (take 10 (s/split (slurp "/usr/share/dict/words") #"\n")))
(def word-list (zipmap words (map (fn[x] (->> x reverse (s/join "")))  words)))

(deftest basic-construct)
  (testing "startup"
    (let [heapobj (heap/init "ccc" 123)]
      ; (map (fn[[x y]] (.insert heapobj x y)) (seq word-list)  )
      (.insert heapobj "aaa" 1)
      (.insert heapobj "bbb" 2)
      (.insert heapobj "ddd" 1)

      (is (= (.getKey heapobj) "aaa" ))
      (is (= (.getKey (.getLeft heapobj)) "ccc" ))
    )

  )
