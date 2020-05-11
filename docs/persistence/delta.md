#Format 'db_delta'.

Single entry storage layout:

- 8B part of hash

- 8B combined
    - 5B (most significant) of total elo difference between white and black
    - 3B of the rest of hash

- 4B packed data
    - 27 most significant bits store the packed reverse move representation
    - next 2 bits store the numerical representation of the game level (human/engine/server)
    - next 2 bits store the numerical represetntation game result (win/loss/draw)
        - exact values can be looked up in src/chess/GameClassification.h
    - 1 bit of padding - always equal to 0

- 4B count
- 4B first game index
- 4B last game index

- 32B total

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

This is currently the richest format.

It is practically limited to 4 billion games (but count may overflow in very rare situations if a common position is repeated many times).
