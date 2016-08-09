(ns b-plus-tree.bst
  (:require [taoensso.timbre :as log]))

;; comparator helpers
(def gt? (comp pos? compare))

(def lt? (comp neg? compare))

(definterface INode
  (getLeft [])
  (getRight [])
  (setLeft [n])
  (setRight [n])
  (getKey [])
  (setKey [k])
  (getVal [])
  (setVal [v])
  (insert [k v])
  (lookup [k])
  (delete [k])
  (delete [k n])
  (inOrder [])
  (export [acc]))

(deftype Node
  [^:volatile-mutable key
   ^:volatile-mutable val
   ^:volatile-mutable ^INode left
   ^:volatile-mutable ^INode right]

  INode
  (getLeft [_] left)
  (getRight [_] right)
  (setLeft [_ v] (set! left v))
  (setRight [_ v] (set! right v))
  (getKey [_] key)

  (setKey [_ k] (set! key k))

  (getVal [_] val)

  (setVal [_ v] (set! val v))

  (lookup [this k]
    ;; check if current root's key matches search key `k`
    (if (= k key)
      val
      (cond
        ;; if both a non-nil right and `k` is greater than key
        (and (gt? k key) right) (.lookup right k)

        ;; if both a non-nil left and `k` is less than key
        (and (lt? k key) left) (.lookup left k))))

  (insert [this k v]
    ;; establish a new node for insertion
    (let [n (Node. k v nil nil)]
      (cond
        ;; inserted key `k` is larger than root node's key
        (gt? k key) (if right             ;; if a right node
                      (.insert right k v) ;; recurse, else
                      (set! right n))     ;; set right to `n`

        ;; the inserted key `k` is less than the root node's key
        (lt? k key) (if left
                      (.insert left k v)
                      (set! left n)))))

  (inOrder [_]
    (lazy-cat
      ;; if there is a left, call inOrder with it as the root
      (log/infof "Left of %s->%s" key val)
      (when left
        (.inOrder left))

      ;; wrap the root's value with a vector
      (vector [key val])
      (log/infof "Right of %s->%s" key val)
      ;; if there is a right, call inOrder with it as the root
      (when right
        (.inOrder right))))


  (export [_ acc]

    (conj acc  [(str key "->" val)
                [(if left
                  (.export left [])
                  (str key "->" val ":left")) (when right (.export right [])) ] ])
    )

  (delete [this k]
    (.delete this k nil))

  (delete [this k parent]
    (letfn [;; a closure to help us set nodes on the parent node
            (set-on-parent [n]
              (if (identical? (.getLeft parent) this)
                (.setLeft parent n)
                (.setRight parent n)))

            ;; a function that finds the largest node in the
            ;; left subtree
            (largest [n]
              (let [right (.getRight n)]
                (when (.getRight right)
                  (largest right))
                right))]

      ;; if we have the target key, we fall into one of three
      ;; conditions
      (if (= k key)
        ;; note that the cond ordering is to ensure that we do
        ;; not match cases such as (or left right) before we
        ;; check (and left right)
        (cond
          ;; 3. two children, the most complex case: here we
          ;;    want to find either the in-order predecessor or
          ;;    successor node and replace the deleted node's
          ;;    value with its value, then clean it up
          (and left right) (let [pred (largest (.getLeft this))]
                             ;; replace the target deletion node
                             ;; with its predecessor
                             (.setKey this (.getKey pred))
                             (.setVal this (.getVal pred))

                             ;; set the deletion key on the
                             ;; predecessor and delete it as a
                             ;; simpler case
                             (.setKey pred k)
                             (.delete this k))

          ;; 1. no children, so we can simply remove the node
          (and (not left) (not right)) (set-on-parent nil)

          ;; 2. one child, so we can simply replace the old node
          ;;    with it
          :else (set-on-parent (or left right)))

        ;; otherwise we recurse, much like `lookup`
        (cond
          ;; if we have both a non-nil right node and `k` is
          ;; greater than key
          (and (gt? k key) right) (.delete right k this)

          ;; if we have both a non-nil left node and `k` is less
          ;; than key
          (and (lt? k key) left) (.delete left k this))))))

(defn root [key val] (Node. key val nil nil))
