# Variable Length Encoding Scheme

## Scheme

When packing bits the underlying integer type is uint8. Packed bits are put in the most significant bits of the output possible. When the value spans 2 bytes first most significant bits are deposited.

Each move consists of these bit packed values:
```
- Index of the piece on the board
    - This field uses just enough bits to encode the value (our_pieces.count() - 1)
    - The piece index is the number of pieces of the same color before it (bb::before).

- Index of the move of the moved piece
    - For a pawn
        - possible_destinations is a bitboard with all possible places the pawn can end up on (include ep square if possible. There is no need to encode it differently, on decoding we know ep_square so we can easly detect en-passants)
        - initial_move_id is the index of the set bit in possible_destinations corresponding to the pawn destination square (in little-endian rank-file square/bitboard mapping). First set bit has index 0, second set bit has index 1, and so on.

        - if the move is a promotion
            - promotion_id = 0 for knigh, 1 for bishop, 2 for rook, 3 for queen
            - move_id = 4 * initial_move_id + promotion_id
            - encode move_id in just enough bits to encode the value (possible_destinations.count() * 4 - 1)
        - if the move is NOT a promotion
            - encode intial_move_id in just enough bits to encode the value (possible_destinations.count() - 1)

    - For a king
        - possible_destinations is a bitboard with pseudo_attacks & ~our_pieces
        - num_castlings is the number of castling rights left for the moving king

        - if the move is castling:
            - castling_id = 0 if (long) or (short and (long not possible)). otherwise 1 (meaning short)
            - move_id = (possible_destinations.count() - 1) + castling_id
            - so the castlings are ordered long -> short, and then we encode the index into possible castlings
        - if the move is NOT castling:
            - move_id is the index of the set bit in possible_destinations corresponding to the destination square (in little-endian rank-file square/bitboard mapping). First set bit has index 0, second set bit has index 1, and so on.

        - encode move_id in just enough bits to encode the value (possible_destinations.count() + num_castlings - 1)

    - For any other piece
        - possible_destinations is a bitboard with attacks & ~our_pieces. We DO NOT exclude attacks that would not be legal moves.
        - The index of the set bit in possible_destinations corresponding to the destination square (in little-endian rank-file square/bitboard mapping). First set bit has index 0, second set bit has index 1, and so on.
        - The number of bits to use is just enough to encode the value (possible_destinations.count() - 1)
```

For example for the first move 1. e4 there's 16 our pieces - requires 4 bits to encode. The moved pawn has index 12. The index of the move is 1 (0 is single push). Therefore the output byte after storing this move will be equal to `0xC8` (with 3 spare low bits).

The decoding process consists of finding the move with the corresponding move index, similarily to how it's done in the move_index.md case, but here the indices are more specialized.