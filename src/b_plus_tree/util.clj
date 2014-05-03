(ns b-plus-tree.util
  "Utility functions.")

; Verbose flag
(def ^:dynamic *verbose* false)

(defmacro dbg
  "Executes the expression x, then prints the expression and its output to
  stderr, while also returning value.
       example=> (dbg (+ 2 2))
       dbg: (+ 2 2) = 4
       4"
  ([x] `(dbg ~x "dbg:" "="))
  ([x msg] `(dbg ~x ~msg "="))
  ([x msg sep] `(let [x# ~x] (println ~msg '~x ~sep x#) x#)))

(defn exit
  "Exit the program with the status and message if given, otherwise status 0."
  ([]                         (System/exit 0))
  ([status]                   (System/exit status))
  ([status msg] (println msg) (System/exit status)))

(defn daemon
  "Executes (func) in a new daemon thread."
  ([func]
     (doto (new Thread func)
       (.setDaemon true)
       (.start))))

(defn print-err
  "Same as print but outputs to stdout."
  ([& more] (binding [*print-readably* nil, *out* *err*] (apply pr more))))

(defn println-err
  "Same as println but outputs to stdout."
  ([& more] (binding [*print-readably* nil, *out* *err*] (apply prn more))))

(defn verbose
  "When *verbose* is true, outputs body to stderr."
  ([& more] (when *verbose* (apply println-err more))))

(defn juxt->
  "Threads the expr through the forms in the [key form] pairs, and returns
  a mapping of key to the result of threading expr through the corresponding
  form. Repeated keys are treated as multiple calls to assoc.

  Credit: arrdem <http://arrdem.com/>"
  ([expr & pairs]
     (let [pairs (partition 2 pairs)
;           [keys fns] (map map [first second] (repeat pairs))
           keys (map first pairs)
           fns (map second pairs)]
       (zipmap keys ((apply juxt fns) expr)))))

(defn in?
  "Returns true if item is in coll, otherwise false."
  ([coll item]
     (some #(= item %) coll)))

(defn unique-strings
  "Returns a seq of unique strings of the given length. "
  ([length]
     (let [formatter (new java.text.DecimalFormat
                          (apply str (repeat length 0)))
           step (fn step [n]
                  (cons (.format formatter n)
                        (-> n inc step lazy-seq)))]
       (lazy-seq (step 0))))
  ([n length]
     (take n (unique-strings length))))
