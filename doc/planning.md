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
- split this sequence at the point where k_i < k < k_{i+1}
- concatenate first half, new key-val pair, last half
- this is the new key-vals pairs sequence
- now convert this into separate keys and vals sequences
```clojure
(let [new-leaf (assoc leaf :keys new-keys :children new-vals)] ...)
```