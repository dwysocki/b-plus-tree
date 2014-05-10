(ns b-plus-tree.seq
  "Sequence functions.")

(defn split-half
  "Splits coll in half.
  If coll has an uneven number of elements, the second half will contain the
  extra element."
  ([coll]
     (let [mid (-> coll count (quot 2))]
       (split-at mid coll))))

(defn split-half-into
  "Splits from-coll in half, putting the results of each into to-coll"
  ([to from]
     (map (partial into to) (split-half from))))

(defn split-center
  "Splits coll into 3 parts, with the center element being its own part.
  The first part is never larger than the last.

  Examples:
    > (split-center (range 5))
    [(0 1) 2 (3 4)]

    > (split-center (range 6))
    [(0 1) 2 (3 4 5)]"
  ([coll]
     (let [mid (-> coll count dec (quot 2))
           [front back] (split-at mid coll)]
       [front (first back) (next back)])))

(defmacro n-range
  "Returns a lazy sequence of n numbers from start (inclusive), by step,
  where start defaults to 0, step to 1, and n to infinity. When step is equal
  to 0, returns an infinite sequence of start. When start is equal to end,
  returns empty list."
  ([] `(range))
  ([n] `(step-range 0 ~n 1))
  ([start n] `(step-range ~start ~n 1))
  ([start n step] `(range ~start (+ ~start (* ~n ~step)) ~step)))
