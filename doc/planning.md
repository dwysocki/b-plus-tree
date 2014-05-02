Planning
========

# April 20, 2014

## Encoding Changes

- need to change how reading from file works
- write single short describing how many bytes follow
- rest of bytes will be a valid node encoding, which
  gloss can decode without further information
- this means not including the type in the information
  which is read directly from the RAF

| Size | Header | Rest |
|:----:|:------:|:----:|
| short | byte | bytes |

# April 23, 2014

## Inserting into non-full leaf

- Start with key-vals pairs.
- split this sequence at the point where `k_i < k < k_{i+1}`
- concatenate first half, new key-val pair, last half
- this is the new key-vals pairs sequence
- now convert this into separate keys and vals sequences
```clojure
(let [new-leaf (assoc leaf :keys new-keys :children new-vals)] ...)
```

# April 24, 2014

## Keyval Serialization
- Going to change serialization of keys/pointers to a seq of keyvals, which
  will be parsed into a map simply using `(apply sorted-map keyvals)`
```clojure
(defcodec node {:type node-types,
                :keyvals keyval-seq})
```
## Future Plan: 
- After insert is finished, work on separate word-scraper library,
  which has functions to fetch words from webpages.
- Also use this library in wiki-path. This will save a lot of code
  copying, but will require messing around with deps.
  - Will allow me to work on both at same time, too. Working on one
    project can improve the other
  - Possible pitfalls when I want to make major changes to
    word-scraper, because I'll have to change two projects. So it is
    probably best to work mainly on b-plus-tree

# May 2, 2014

## Delayed Persistence
- Instead of writing each node to disc each time one is altered, have
  a hash-map, mapping RAF offsets to nodes which have already been
  read and/or altered
- Whenever a node needs to be read off the disc, first check if its
  offset is already in the hash-map, and use the in-memory node
  instead if one exists
- Instead of simply using `assoc` to add new nodes to the map, write a
  function which checks the number of elements in the map, and if it
  reaches a certain threshold (a parameter to the function), write
  half of the nodes at random to disc, and `dissoc` them from the map
  after they've been written
- When all operations are finished, write the entire hash-map to disc,
  and let it be garbage-collected
