(ns b-plus-tree.heap
  (:require [taoensso.timbre :as log]))

(def gt? (comp pos? compare))

(def lt? (comp neg? compare))

(definterface INode
  (getLeft [])
  (getRight [])

  (setLeft [n])
  (setRight [n])

  (getKey [])
  (getValue [])

  (setKey [v])
  (setValue [v])

  (insert [ k v])
  (lookup [ k])
  (inOrder [])
  (export [acc])
  (setNode [k v])
  (compParent [child])
  (getNode [])
  (getParent [])
  (getDepth [])
  (printed [])
  (getMarker [])
  (write [writer])
  )
;
; (defn iterate [o]
;   (fn step [o acc]
;     (lazy-seq
;
;       (when (.getLeft o)
;         (step (.getLeft o) (conj acc [(.getKey o) (.getValue o)] ))))
;       ; (when (.getRight o)
;     ;   (recur (.getRight o)))
;     )
;
;   )

(defn compParent [nparent nchild]
  (let [[ck cv] (.getNode nchild)
        [pk pv] (.getNode nparent)]
    (when (lt? (.getKey nchild) (.getKey nparent))
       ; (log/infof "Comparing parent \nP:[%s] -> [%s] " nparent nchild)

      (doto nchild (.setKey pk) (.setValue pv))
      (.setNode nparent ck cv)
      )

      nil
      ))

(defn node-iter
  [root writer]
  (loop [i 0
         nodes [root]]
     (when-not (empty? nodes)
       (let [subnode-writer
               (fn [p]
                 (when p
                   (let [l (.getLeft p)
                         r (.getRight p)]
                     (when l
                       (.append writer (format "\"%s\"->\"%s\"; \n" (.getKey p) (.getKey l))))
                     (when r
                       (.append writer (format "\"%s\"->\"%s\"; \n" (.getKey p) (.getKey r))))
                   [l r])))]

            (recur (inc i)
                    (mapcat subnode-writer nodes))
         )))
  )

(deftype Node
  [^:volatile-mutable key
   ^:volatile-mutable val
   ^:volatile-mutable ^INode parent
   ^:volatile-mutable ^INode left
   ^:volatile-mutable ^INode right
   ^:volatile-mutable depth
   marker]

   Object
   (toString [_]
     (let [separator (apply str (repeat (* 1 depth) "-") )]
       (str (format "::%s::N#%s->%s" depth key val)
            ; (when left (format "\n%sN#L%s" separator (.toString left)  ))
            ; (when right (format "\n%sN#R%s" separator (.toString right)  ))
            )))
    clojure.lang.Indexed
    (nth [this i] (.getLeft this))
    (nth [this i not-found] (or (.getLeft this) not-found))

    clojure.lang.Seqable
    (seq [this] [(.getLeft this) (.getRight this)])

   INode
   (write [this writer]
     (node-iter this writer)
     )


   (printed [this]
     (let [o (tree-seq identity (fn[x] [(.getLeft x) (.getRight x)]) this)]
        (mapv (fn [x] (when x
                        (let [depth (.getDepth x) key (.getKey x) val (.getValue x)
                              marker (.getMarker x)
                              separator (apply str (take 300 (repeat (* 1 depth) "-")) )]
            (println (format ":%s:%s[%s]#%s->%s" depth separator marker key val ))) ))
            o )))

   (getMarker [_] marker)
   (getLeft [_] left)
   (getRight [_] right)
   (getDepth [_] depth)
   (getParent [_] parent)

   (setLeft [_ v] (set! left v))
   (setRight [_ v] (set! right v))

   (getKey [_] key)
   (getValue [_] val)

   (setKey [_ v] (set! key v))
   (setValue [_ v] (set! val v))

   (lookup [this k]
     (if (= k key)
        this
        ( or (when left (.lookup left k))
              (when right (.lookup right k))
     )
     ))

   (inOrder [_]
     (lazy-seq
       ;; if there is a left, call inOrder with it as the root
      ;  (log/infof "Left of %s->%s" key val)
       (when left
         (.inOrder left))

       ;; wrap the root's value with a vector
       (vector [key val])
      ;  (log/infof "Right of %s->%s" key val)
       ;; if there is a right, call inOrder with it as the root
       (when right
         (.inOrder right))))

   (setNode [this key val]

     (.setKey this key)
     (.setValue this val)
     (when parent
       (compParent parent this)
     ))

   (getNode [this]
     [key val]
    )


   (insert [this key val]
     (let [pick (mod (hash key) 2)]
      (if-not left
        (let [n (Node. key val this nil nil (inc depth) :L)]
          (set! left n)
          (compParent this n))
        (if-not right
          (let [n (Node. key val this nil nil (inc depth) :R)]
            (set! right n)
            (compParent this n))
          (condp = pick
            0 (.insert left key val)
            1  (.insert right key val)
            )
          ))))

    (export [this acc]
      (conj acc  [(str key "->" val)
                  [(if left
                    (.export left [])
                    "")
                    (when right
                      (.export right [])) ] ])
      )
     )

; (def heapm (Node. "test" 123 nil nil nil) )
(defn init [k v] (Node. k v nil nil nil 0 :0))
(defn bind [h] (-> h (.insert "k" "v") (.insert "m" "s") ))
