#Format 'db_alpha'.

Single entry storage layout:

- 16B key (uint32_t key[4])

    - key[0], key[1], key[2] and the 5 most significant bits of key[3] store the hash of the position
    - 27 least significant bits of key[3] stores the packed reverse move representation

- 4B first game index

There exist two orderings of the entries.

1. By the whole key (including reverse move)
2. By key part that contains hash.

For each position there is one entry stored. Entries are ordered by the first predicate.

Two indexes are kept, one for each ordering type.

The second order is stable with respect to game index - ie. the first entry that is equal with respect to this order contains the actual first game index.

The first order is not stable with respect to the game index - ie. it's not possible to extract the first game index for transpositions without going through all the positions (which is infeasible).

Using the first index allows extracting position instances that match both the position and the reverse move - so direct continuations.
Using the second index allows extracting all the instances of the position - so including transpositions.

Divides positions into 9 partitions, each one with it's own directory:

- /engine/w
- /engine/d
- /engine/l
- /human/w
- /human/d
- /human/l
- /server/w
- /server/d
- /server/l

Each partition can contain many files.

Querying a single position requires querying from all 9 directories.

This is a legacy format. Beta format is better in every way.
