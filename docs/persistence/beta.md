#Format 'db_beta'.

Single entry storage layout:

- 16B key (uint32_t key[4])

    - key[0], key[1], key[2] store the hash of the position
    - 27 most significant bits of key[3] stores the packed reverse move representation
    - next 2 bits store the numerical representation of the game level (human/engine/server)
    - next 2 bits store the numerical represetntation game result (win/loss/draw)
        - exact values can be looked up in src/chess/GameClassification.h
    - 1 bit of padding - always equal to 0

- 8B of combined position count and first game offset in the headers file
    - from lowest to highest bits:
    - 6 lowest bits used by the discriminator encoding value N saying how many bits are used by game count
    - N bits containing the number of games (A) with this key
    - 58-N bits containing the first game offset (B)
    - This packing is based on the assumption that when there is more games it is more likely
      that the first game appeared earlier, so that A\*B < 2^58.

There exist three orderings of the entries.

1. By the whole key (including reverse move, level, and result)
2. By the part that contains hash and reverse move
3. By key part that contains hash.

For each position there is one entry stored. Entries are ordered by the first predicate.

Only one index is kept, with the third ordering.

First ordering is used only for merging.

When performing a query all entries within an index block are iterated and compared according to either second or third order (depending whether we want transpositions or not)

The is only one partition - directory /data

The partition can contain many files.

It's currently practically limited to 4 billion games by the header storage scheme, but it may change in the future.

This format is more efficient the more duplicated positions there are. Empirical data shows usage of an average o 17.5 bytes per position when there is ~6 billion positions.
