#Format 'db_delta'.

Single entry storage layout:

- 12B key
    - 9B hash
        - with 4 trillion positions it is expected to have in the order of a hundred collisions

    - 3B packed data
        - 20 bits of packed reverse move - uses a very compact encoding, utilizes move_index
        - next 2 bits store the numerical representation of the game level (human/engine/server)
        - next 2 bits store the numerical represetntation game result (win/loss/draw)

- 4B entry
    - 4B count

- 16B total

There exist three orderings of the entries.

1. By the whole key (including reverse move, level, and result)
2. By the part that contains hash and reverse move
3. By key part that contains hash.

For each unique position there is one entry stored. Entries are ordered by the first predicate.

Only one index is kept, with the third ordering.

First ordering is used only for merging.

When performing a query all entries within an index block are iterated and compared according to either second or third order (depending whether we want transpositions or not)

There is only one partition - directory /data

The partition can contain many files.

This is currently the most bare-bones format. It has the smallest footprint but doesn't allow first game queries.

It is practically limited to 4 billion games (but count may overflow in very rare situations if a common position is repeated many times).
