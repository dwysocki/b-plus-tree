(ns b-plus-tree.test-utils
  "Functions needed for unit tests")

(defmacro with-private-fns
  "Refers private fns from ns and runs tests in context.
  Author: Chris \"Chouser\" Houser <https://github.com/Chouser>"
  ([[ns fns] & tests]
     `(let ~(reduce #(conj %1 %2 `(ns-resolve '~ns '~%2)) [] fns)
        ~@tests)))
