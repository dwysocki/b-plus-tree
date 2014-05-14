(ns b-plus-tree.remove-test
  (:require [clojure.java.io :as io]
            [b-plus-tree core io])
  (:use clojure.test
        b-plus-tree.test-utils))

(def leaves
  {1000 {:type :leaf
         :key-ptrs (sorted-map "a" 9000
                               "b" 9100
                               "bb" 10000
                               "bc" 10100)
         :prev -1
         :next 1100
         :offset 1000},
   1100 {:type :leaf
         :key-ptrs (sorted-map "c" 9200
                               "d" 9300
                               "dd" 10200
                               "de" 10300)
         :prev 1000
         :next 1200
         :offset 1100},
   1200 {:type :leaf
         :key-ptrs (sorted-map "e" 9400
                               "f" 9500
                               "ff" 10400
                               "fg" 10500)
         :prev 1100
         :next 1300
         :offset 1200},
   1300 {:type :leaf
         :key-ptrs (sorted-map "g" 9600
                               "h" 9700
                               "hh" 10600
                               "hm" 10700)
         :prev 1200
         :next -1
         :offset 1300}})

(def parents
  {500 {:type :internal
        :key-ptrs (sorted-map "c" 1000
                              "e" 1100
                              "g" 1200)
        :last 1300
        :offset 500}
   100 {:type :root-nonleaf
        :key-ptrs (sorted-map "a" 200)
        :last 500
        :offset 100}})

(def root
  (parents 100))

(def parent
  (parents 500))

(def stack
  [root parent])

(def header
  {:count 16
   :free  10000
   :order 5
   :key-size 32
   :val-size 32
   :page-size 100
   :root 100})

(def cache
  (merge leaves parents))

(with-private-fns [b-plus-tree.core [siblings]]
  (deftest sibling-test
    (testing "testing sibling function"
      (is (= (siblings (leaves 1000) parent nil cache)
             [nil (leaves 1100)]))
      (is (= (siblings (leaves 1100) parent nil cache)
             [(leaves 1000) (leaves 1200)]))
      (is (= (siblings (leaves 1200) parent nil cache)
             [(leaves 1100) (leaves 1300)]))
      (is (= (siblings (leaves 1300) parent nil cache)
             [(leaves 1200) nil])))))

(with-private-fns [b-plus-tree.core [steal-merge]]
  (deftest steal-test
    (testing "testing steal function"
      (println (steal-merge (leaves 1200) "zzzzz" stack nil header cache)))))


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

              _ (println "BEFORE:")
              _
              (b-plus-tree.core/print-leaf-keys raf header
                                                :cache cache)
              
              [remaining, missing, header, cache]
              (loop [key-vals  (into (sorted-map) key-vals)
                     header    header
                     cache     cache
                     remaining {}
                     missing   #{}
                     n         0]
                ;; (println "header:" header)
                ;; (println "remaining:" (count remaining))
                (if-let [[k v] (first key-vals)]
                  (let [[remaining missing header cache]
                        (try
                          (let [[header cache]
                                (b-plus-tree.core/delete k raf header
                                                         :cache cache)]
                            (println "removed" k)
                            [remaining, missing, header, cache])

                          (catch UnsupportedOperationException e
                            (println (.getMessage e))
                            [(assoc remaining k v), missing, header, cache])
                          (catch clojure.lang.ExceptionInfo e
                            (println (.getMessage e))
                            [remaining,
                             (apply conj missing
                                    (:missing-keys (ex-data e))),
                             header,
                             cache])
                          (catch Exception e
                            (println "n:" n)
                            (throw e)))]

                    (recur (rest key-vals)
                           header
                           cache
                           remaining
                           missing
                           (inc n)))
                                        ; reached the end
                  [remaining, missing, header, cache]))]

          (println "AFTER:")
          (b-plus-tree.core/print-leaf-keys raf header
                                            :cache cache)

          (is (b-plus-tree.core/map-equals? remaining raf header
                                            :cache cache))
          
          (b-plus-tree.io/write-cache cache raf)
          (is (b-plus-tree.core/map-equals? remaining raf header))

          (println "finding problem keys")
          (doseq [k missing]
            (let [[v _] (b-plus-tree.core/find-val k raf header
                                                   :cache cache)]
              (if v
                (println "Found" v)
                (println "Could not find" v))))

          (println "size" (:count header))))
      (delete-file))))
