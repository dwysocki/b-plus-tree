(defproject b-plus-tree "0.3.0"
  :description "A B+ Tree implemented in Clojure."
  :url "https://github.com/Rosnec/b-plus-tree"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                  [com.taoensso/timbre "4.7.3"]
                 [gloss "0.2.6"]]

   :profiles {:uberjar {:aot :all}

           :dev { :source-paths ["dev"]
            :ultra {:stacktraces false}
           :dependencies [ [org.clojure/tools.namespace "0.2.11"]
                           [rhizome "0.2.7"]]}}
                 )
