(ns b-plus-tree.linked
  ; (:require )
  )


(definterface INode
  (getCar [])
  (setCar [x])
  (getCdr [])
  (setCdr [n])
  (reverse []))

(deftype Node
  [^:volatile-mutable car ^:volatile-mutable ^INode cdr]
  INode
  (getCar [_] car)
  (setCar [_ x] (set! car x))
  (getCdr [_] cdr)
  (setCdr [_ n] (set! cdr n))
  (reverse [this]
    (loop [cur this new-head nil]
      (if-not cur
        (or new-head this)
        (recur (.getCdr cur) (Node. (.getCar cur) new-head)))))

  clojure.lang.Seqable
  (seq [this]
    (loop [cur this
           acc ()]
      (if-not cur
        acc
        (recur (.getCdr cur) (conj acc (list (.getCar cur))) )
        )))
    clojure.lang.ITransientCollection
        )
