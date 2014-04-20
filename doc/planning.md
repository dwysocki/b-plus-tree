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
