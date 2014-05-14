(ns b-plus-tree.remove-test
  (:require [clojure.java.io :as io]
            [b-plus-tree core io])
  (:use clojure.test
        b-plus-tree.test-utils))


(deftest no-merge-test
  (testing "removing from B+ Tree without any merges"
    (binding [order 32]
      (delete-file)
      (new-tree)
      (with-open [raf (get-raf)]
        (let [header (get-header raf)
              
              key-vals (unsorted-numbered-strings 1000)
              
              [header cache]
              (b-plus-tree.core/insert-all key-vals raf header)

;              header (assoc header :count (inc (:count header)))
              _
              (b-plus-tree.core/print-leaf-keys raf header
                                                :cache cache)
              
              [remaining header cache]
              (loop [key-vals  key-vals
                     header    header
                     cache     cache
                     remaining {}
                     n         0]
                ;; (println "header:" header)
                ;; (println "remaining:" (count remaining))
                (if-let [[k v] (first key-vals)]
                  (let [[remaining header cache]
                        (try
                          (let [[header cache]
                                (b-plus-tree.core/delete k raf header
                                                         :cache cache)]
                            [remaining header cache])

                          (catch UnsupportedOperationException e
                            (println (.getMessage e))
                            [(assoc remaining k v), header, cache])
                          (catch Exception e
                            (println "n:" n)
                            (throw e)))]
                    (recur (rest key-vals) header cache remaining (inc n)))
                  ; reached the end
                  [remaining header cache]))]

          (comment
            (b-plus-tree.core/print-leaf-keys raf header
                                              :cache cache))

          (is (b-plus-tree.core/map-equals? remaining raf header
                                            :cache cache))
          
          (b-plus-tree.io/write-cache cache raf)
          (is (b-plus-tree.core/map-equals? remaining raf header))

          (println "size" (:count header))))
      (delete-file))))
