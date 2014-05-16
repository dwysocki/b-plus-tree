(ns b-plus-tree.remove-test
  (:require [clojure.java.io :as io]
            [b-plus-tree core io nodes])
  (:use clojure.test
        b-plus-tree.test-utils))

(def leaves
  {; leftmost
   1000 {:type :leaf
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
         :next 2100
         :offset 1300}
   ; middle
   2100 {:type :leaf
         :key-ptrs (sorted-map "i" 2110)
         :prev 1300
         :next 2200
         :offset 2100},
   2200 {:type :leaf
         :key-ptrs (sorted-map "j" 2210)
         :prev 2100
         :next 2300
         :offset 2200},
   2300 {:type :leaf
         :key-ptrs (sorted-map "k" 2310)
         :prev 2200
         :next 3100
         :offset 2300}
   ; rightmost
   3100 {:type :leaf
         :key-ptrs (sorted-map "l" 3110)
         :prev 2300
         :next 3200
         :offset 3100},
   3200 {:type :leaf
         :key-ptrs (sorted-map "m" 3210)
         :prev 3100
         :next 3300
         :offset 3200},
   3300 {:type :leaf
         :key-ptrs (sorted-map "n" 3310)
         :prev 3200
         :next -1
         :offset 3300}})

(def parent-nodes
  {100 {:type :root-nonleaf
        :key-ptrs (sorted-map "i" 500
                              "l" 2000)
        :last 3000
        :offset 100},
   500 {:type :internal
        :key-ptrs (sorted-map "c" 1000
                              "e" 1100
                              "g" 1200)
        :last 1300
        :offset 500},
   2000 {:type :internal
         :key-ptrs (sorted-map "j" 2100
                               "k" 2200)
         :last 2300
         :offset 2000},
   3000 {:type :internal
         :key-ptrs (sorted-map "m" 3100
                               "n" 3200)
         :last 3300
         :offset 3000}})

(def root
  (parent-nodes 100))

(def parent
  (parent-nodes 500))

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
  (merge leaves parent-nodes))

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

(comment
  (with-private-fns [b-plus-tree.core [steal-merge]]
    (deftest steal-test
      (testing "testing steal function"
        (println (steal-merge (leaves 1200) "zzzzz" stack nil header cache))))))



(with-private-fns [b-plus-tree.core [merge-prev-leaf merge-next-leaf
                                     merge-internal cache-node]]
  (deftest merge-test
    (testing "testing merge functions"
      (println "prev-merging 2nd leaf with 1st")
      (println (select-keys (merge-prev-leaf (leaves 1100)
                                             (leaves 1000)
                                             "zzzz"
                                             stack
                                             nil
                                             cache)
                            [100 500 1000 1100 1200 1300]))
      (println "prev-merging 3rd leaf with 2nd")
      (println (select-keys (merge-prev-leaf (leaves 1200)
                                             (leaves 1100)
                                             "zzzz"
                                             stack
                                             nil
                                             cache)
                            [100 500 1000 1100 1200 1300]))
      (println "next-merging 1st leaf with 2nd")
      (println (select-keys (merge-next-leaf (leaves 1000)
                                             (leaves 1100)
                                             "zzzz"
                                             stack
                                             nil
                                             cache)
                            [100 500 1000 1100 1200 1300]))
      (println "next-merging 2nd leaf with 3rd")
      (println (select-keys (merge-next-leaf (leaves 1100)
                                             (leaves 1200)
                                             "zzzz"
                                             stack
                                             nil
                                             cache)
                            [100 500 1000 1100 1200 1300]))
      (println "next-merging 2nd leaf with 3rd, after removing first key")
      (let [node (b-plus-tree.nodes/node-dissoc (leaves 1100) "c")
            cache (cache-node node nil cache)]
        (try
          (println (select-keys (merge-next-leaf
                                 node
                                 (leaves 1200)
                                 "c"
                                 stack
                                 nil
                                 cache)
                                [100 500 1000 1100 1200 1300]))
          (catch clojure.lang.ExceptionInfo e
            (println "EEEEEEEEE" (ex-data e)))))
      
      (println "merging 1st internal with 2nd")
      (println (second (merge-internal
                        (parent-nodes 500)
                        (parent-nodes 2000)
                        root
                        nil
                        cache))))))

(comment
  (with-private-fns [b-plus-tree.core [cache-nodes merge-internal]]
    (deftest merge-internal-test
      (testing "merging internal nodes"
        (let [root {:type :root-nonleaf
                    :key-ptrs (sorted-map "c" 200
                                          "e" 300)
                    :last 400
                    :offset 100}
              low-internal {:type :internal
                            :key-ptrs (sorted-map "b" 500)
                            :last 600
                            :offset 200}
              mid-internal {:type :internal
                            :key-ptrs (sorted-map "d" 700)
                            :last 800
                            :offset 300}
              high-internal {:type :internal
                             :key-ptrs (sorted-map "d" 900)
                             :last 1000
                             :offset 400}
              a-leaf {:type :leaf
                      :key-ptrs (sorted-map "a" 2000)
                      :prev -1
                      :next 600
                      :offset 500}
              b-leaf {:type :leaf
                      :key-ptrs (sorted-map "b" 2100)
                      :prev 500
                      :next 700
                      :offset 600}
              c-leaf {:type :leaf
                      :key-ptrs (sorted-map "c" 2200)
                      :prev 600
                      :next 800
                      :offset 700}
              d-leaf {:type :leaf
                      :key-ptrs (sorted-map "d" 2300)
                      :prev 700
                      :next 900
                      :offset 800}
              e-leaf {:type :leaf
                      :key-ptrs (sorted-map "e" 2400)
                      :prev 800
                      :next 1000
                      :offset 900}
              f-leaf {:type :leaf
                      :key-ptrs (sorted-map "f" 2500)
                      :prev 900
                      :next -1
                      :offset 1000}
              initial-cache (cache-nodes
                             [root low-internal mid-internal high-internal
                              a-leaf b-leaf c-leaf d-leaf e-leaf f-leaf]
                             nil
                             {})
              [_ cache] (merge-internal low-internal mid-internal root
                                        nil initial-cache)
              expected-cache (-> initial-cache
                                 (dissoc 200)
                                 (assoc 300
                                   {:type :internal
                                    :key-ptrs (sorted-map "b" 500
                                                          "c" 600
                                                          "d" 700)
                                    :last 800
                                    :offset 300
                                    :altered? true})
                                 (assoc 100
                                   {:type :root-nonleaf
                                    :key-ptrs (sorted-map "e" 300)
                                    :last 400
                                    :offset 100
                                    :altered? true}))]
          (is (= cache expected-cache)))))))

(comment
  (with-private-fns [b-plus-tree.core [cache-nodes merge-root-leaf]]
    (deftest merge-root-leaf-test
      (testing "merging root with leaves"
        (let [low-leaf  {:type :leaf
                         :key-ptrs (sorted-map "a" 1000
                                               "b" 1100)
                         :prev -1
                         :next 300
                         :offset 200},
              high-leaf {:type :leaf
                         :key-ptrs (sorted-map "c" 1200
                                               "d" 1300)
                         :prev 200
                         :next -1
                         :offset 300}
              root {:type :root-nonleaf
                    :key-ptrs (sorted-map "c" 200)
                    :last 300
                    :offset 100}
              cache (cache-nodes [root low-leaf high-leaf] nil {})
                                        ; what the cache should become after merging
              expected-cache
              {100 {:type :root-leaf
                    :key-ptrs (sorted-map "a" 1000
                                          "b" 1100
                                          "c" 1200
                                          "d" 1300)
                    :offset   100
                    :altered? true}}
              cache (merge-root-leaf low-leaf high-leaf root nil cache)]
          (is (= cache expected-cache)))))))

(comment
  (with-private-fns [b-plus-tree.core [cache-nodes merge-root-internal]]
    (deftest merge-root-internal-test
      (testing "merging root with internal nodes"
        (let [root {:type :root-nonleaf
                    :key-ptrs (sorted-map "c" 200)
                    :last 300
                    :offset 100}
              low-internal {:type :internal
                            :key-ptrs (sorted-map "b" 400)
                            :last 500
                            :offset 200}
              high-internal {:type :internal
                             :key-ptrs (sorted-map "d" 600)
                             :last 700
                             :offset 300}
              a-leaf {:type :leaf
                      :key-ptrs (sorted-map "a" 1000)
                      :prev -1
                      :next 500
                      :offset 400}
              b-leaf {:type :leaf
                      :key-ptrs (sorted-map "b" 1100)
                      :prev 400
                      :next 600
                      :offset 500}
              c-leaf {:type :leaf
                      :key-ptrs (sorted-map "c" 1200)
                      :prev 500
                      :next 700
                      :offset 600}
              d-leaf {:type :leaf
                      :key-ptrs (sorted-map "d" 1300)
                      :prev 600
                      :next -1
                      :offset 700}
              initial-cache (cache-nodes [root low-internal high-internal
                                          a-leaf b-leaf c-leaf d-leaf]
                                         nil
                                         {})
              cache (merge-root-internal low-internal high-internal root
                                         nil initial-cache)
              expected-cache (-> initial-cache
                                 (dissoc 200 300)
                                 (assoc 100
                                   {:type :root-nonleaf
                                    :key-ptrs (sorted-map "b" 400
                                                          "c" 500
                                                          "d" 600)
                                    :last 700
                                    :offset 100
                                    :altered? true}))]
          (is (= cache expected-cache)))))))
(comment
  (with-private-fns [b-plus-tree.core [cache-nodes
                                       steal-prev-internal
                                       steal-next-internal]]
    (deftest steal-prev-internal-test
      (testing "testing steal from previous internal node function"
        (let [root {:type :root-nonleaf
                    :key-ptrs (sorted-map "d" 200)
                    :last 300
                    :offset 100
                    :altered? true}
              low-internal {:type :internal
                            :key-ptrs (sorted-map "b" 400
                                                  "c" 500)
                            :last 600
                            :offset 200
                            :altered? true}
              high-internal {:type :internal
                             :key-ptrs (sorted-map "e" 700)
                             :last 800
                             :offset 300
                             :altered? true}
              a-leaf {:type :leaf
                      :key-ptrs (sorted-map "a" 2000)
                      :prev -1
                      :next 500
                      :offset 400}
              b-leaf {:type :leaf
                      :key-ptrs (sorted-map "b" 2100)
                      :prev 400
                      :next 600
                      :offset 500}
              c-leaf {:type :leaf
                      :key-ptrs (sorted-map "c" 2200)
                      :prev 500
                      :next 700
                      :offset 600}
              d-leaf {:type :leaf
                      :key-ptrs (sorted-map "d" 2300)
                      :prev 600
                      :next 800
                      :offset 700}
              e-leaf {:type :leaf
                      :key-ptrs (sorted-map "e" 2400)
                      :prev 700
                      :next -1
                      :offset 800}
              initial-cache (cache-nodes
                             [root low-internal high-internal
                              a-leaf b-leaf c-leaf d-leaf e-leaf]
                             nil
                             {})
              [_ cache] (steal-prev-internal high-internal low-internal root
                                             nil initial-cache)
              expected-cache (assoc initial-cache
                               100 {:type :root-nonleaf
                                    :key-ptrs (sorted-map "c" 200)
                                    :last 300
                                    :offset 100
                                    :altered? true}
                               200 {:type :internal
                                    :key-ptrs (sorted-map "b" 400)
                                    :last 500
                                    :offset 200
                                    :altered? true}
                               300 {:type :internal
                                    :key-ptrs (sorted-map "d" 600
                                                          "e" 700)
                                    :last 800
                                    :offset 300
                                    :altered? true})
              _ (is (= cache expected-cache))
                                        ; now try stealing back
              {low-internal 200, high-internal 300, root 100} cache
              [_ cache] (steal-next-internal low-internal high-internal root
                                             nil cache)]
          (is (= cache initial-cache)))))))

(comment
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

                [remaining, missing, header, cache]
                (loop [key-vals  (into (sorted-map) key-vals)
                       header    header
                       cache     cache
                       remaining {}
                       missing   #{}
                       n         0]
                  (if-let [[k v] (first key-vals)]
                    (let [[remaining missing header cache]
                          (try
                            (let [[header cache]
                                  (b-plus-tree.core/delete k raf header
                                                           :cache cache)]
                              [remaining, missing, header, cache])

                            (catch UnsupportedOperationException e
                              [(assoc remaining k v), missing, header, cache])
                            (catch clojure.lang.ExceptionInfo e
                              (println (.getMessage e) (ex-data e))
                              (case (:cause (ex-data e))
                                :missing-keys
                                [remaining,
                                 (apply conj missing
                                        (:missing-keys (ex-data e))),
                                 header,
                                 cache]

                                [remaining, missing, header, cache]))
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

            (is (b-plus-tree.core/map-equals? remaining raf header
                                              :cache cache))
            
            (b-plus-tree.io/write-cache cache raf)
            (is (b-plus-tree.core/map-equals? remaining raf header))

            (doseq [k missing]
              (let [[v _] (b-plus-tree.core/find-val k raf header
                                                     :cache cache)]
                (if v
                  (println "Found missing key" v)
                  (println "Could not find missing key" v))))))
        (delete-file)))))


(deftest delete-test
  (testing "testing delete function"
    (binding [order 32]
      (delete-file)
      (new-tree)
      (with-open [raf (get-raf)]
        (let [header (get-header raf)

              key-vals
              (numbered-strings 1000)
              
              half (-> key-vals count (* 3/4))

              half-keys (take half
                              (shuffle (keys key-vals)))

              [header cache]
              (b-plus-tree.core/insert-all key-vals raf header)

              _
              (is (b-plus-tree.core/map-equals? key-vals raf header
                                                :cache cache))

              [header cache]
              (b-plus-tree.core/delete-all half-keys raf header
                                           :cache cache)

              remaining (apply dissoc key-vals half-keys)
              ]
          (println "size:" (:count header))
          (println "depth:" (b-plus-tree.core/depth raf header))
          (println "root:" (cache (:root header)))
          
          (is (b-plus-tree.core/map-equals? remaining raf header))
          
          (println "now let's see what we've got")
          (b-plus-tree.core/print-leaf-keys raf header))))))

(comment
  (deftest thorough-delete-test
    (testing "deleting 1 items at a time and checking map equality"
      (binding [order 32]
        (delete-file)
        (new-tree)
        (with-open [raf (get-raf)]
          (let [header (get-header raf)

                key-vals
                (unsorted-numbered-strings 1000)
                
                [header cache]
                (b-plus-tree.core/insert-all key-vals raf header)

                _
                (is (b-plus-tree.core/map-equals? key-vals raf header
                                                  :cache cache))]
            (b-plus-tree.io/write-cache cache raf)
            (loop [header   header
                   cache    cache
                   key-vals key-vals
                   n        0]
              (when-let [[k v] (first key-vals)]
                (let [[header cache] (b-plus-tree.core/delete k raf header
                                                              )
                      key-vals (dissoc key-vals k)]
                  (b-plus-tree.io/write-cache cache raf)
                                        ;                (println n)
                  (is (b-plus-tree.core/map-equals? key-vals raf header
                                                    ))
                  (recur header cache key-vals (inc n)))))))))))
