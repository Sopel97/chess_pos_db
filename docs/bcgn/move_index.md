# Move Index Specification

Move index can have either 1 byte or 2 bytes depending on the position.
Whether 1 or 2 bytes is used is determined solely by the position and the criteria are described near the end of this specification in "Determining the size of the result".
This means the position has to be provided to encode/decode a move.
The conversion process is the same regardless of the position.

## Definitions

Square are encoded in little-endian rank-file mapping.
I.e. in order a1, b1, c1, ..., a2, b2, c2, ..., f8, g8, h8

A Bitboard is a 64 bit unsigned integer with bit indices corresponding to
little-endian rank-file square mapping.
I.e. the least significant bit corresponds to a1, most significant bit corresponds to h8
and the order is a1, b1, c1, ...

`before_bb(Square)` is a Bitboard representing all squares with index less than Square. So for example for a6 it would be all square from a1 to a5 inclusive.

`max_moves_on_empty_board(PieceType)` describes the maximum number of moves a piece can make on an empty board.
```
max_moves_on_empty_board(pawn) == 12
max_moves_on_empty_board(knight) == 8
max_moves_on_empty_board(bishop) == 13
max_moves_on_empty_board(rook) == 14
max_moves_on_empty_board(queen) == 27
max_moves_on_empty_board(king) == 8
```

The following are not defined for pawns.

`destinations_bb(PieceType, From)` is the Bitboard of the pseudo attacks for the piece of type `PieceType` on an empty board when it is placed on square `From`.

`destination_square(PieceType, From, Idx)` is a `Idx`-th (0-based) set square in the `destinations_bb(PieceType, From)`

`destination_index(PieceType, From, To)` is specified for each square `To` such that `To` is contained in `destinations_bb(PieceType, From)`. The value is `K` such that the `K`-th (0-indexed) set bit in `destinations_bb(PieceType, From)` corresponds to the `To` square. In other words it is a (0-based) index into the set bits in `destinations_bb(PieceType, From)` corresponding to the index of `To` square. In yet other words it's an inverse of `destination_square`.

For pawns we define (in pseudo-code):
```
pawn_destination_index(move, side_to_move):
{
    if (side_to_move == WHITE)
    {
        // capture left - 7 - 7 = 0
        // single straight - 8 - 7 = 1
        // capture right - 9 - 7 = 2
        // double move - 16 - 7 = 9
        idx = move.to.index - move.from.index - 7
    }
    else
    {
        idx = move.from.index - move.to.index - 7
    }

    if (move.promoted_piece.type != NONE)
    {
        idx <<= 2 // two lowest bits will be used for storing promotion type
        idx +=
            0 if KNIGHT
            1 if BISHOP
            2 if ROOK
            3 if QUEEN
    }

    return idx
}
```

```
destination_index_to_pawn_move(pos, index, from_sq, side_to_move):
{
    relative_from_rank =
        side_to_move == WHITE
        ? from.rank
        : rank8 - from.rank

    promoted_piece = NONE
    move_type = NORMAL
    if (relative_from_rank == rank7)
    {
        promoted_piece_type_idx = index & 3;
        promoted_piece_type =
            KNIGHT if 0
            BISHOP if 1
            ROOK if 2
            QUEEN if 3

        promoted_piece = Piece(promoted_piece_type, side_to_move)
        index >>= 2
        move_type = PROMOTION
    }

    offset = index + 7
    if (side_to_move == BLACK) offset = -offset // because we don't flip the board
    to_sq = from_sq + offset

    if (relative_from_rank != rank7 && to_sq == pos.epSquare)
    {
        type = EN_PASSANT
    }

    return Move(from_sq, to_sq, move_type, promoted_piece)
}
```

## Converting Move to Move Index

Move index computation will be presented in text and pseudo code forms.
The result is a 16 bit integer.

Each piece on the board gets `max_moves_on_empty_board(pt)` values to encode its move
but this value has to be placed at a correct offset (so we know which piece it corresponds to).
So the value space is divided based on what pieces are present on the board.
Each piece has a value space block of length max_moves_on_empty_board(pt) assigned to it.
Each subsequent value block starts at last value in the previous block + 1.
The value space blocks are ordered by move type/piece type in the following way:

- We always have value space block for 2 castling moves (0 or 1)
- We always have value space block for 8 king moves (2 to 9)
- We have value space block for each pawn on the board.
    Within a block we go through pieces of this type in ascending square order.
- value space block for each knight on the board.
- value space block for each bishop on the board.
- value space block for each rook on the board.
- value space block for each queen on the board.

For castling and king moves it is easy to see what the move_index is.
For pawns we use special encoding. `move_index = value_space_block_offset + pawn_destination_index(move, side_to_move)`. This is best described with pseudo-code below.
For any other pice the `move_index` is the `value_space_block_offset + destination_index(moved_piece.type, from_sq, to_sq)`

Pseudocode for conversion from move to move index:
```
pos - the position on which the move is being made
Let the move be a 4-tuple (from_sq, to_sq, move_type, promoted_piece)

// Then when decoding we can find the block the value corresponds to
// and effectively the piece it corresponds to.

if (move_type == CASTLE)
{
    return 0 if king side castling
    return 1 if queen side castling
}

moved_piece = pos.pieceAt(from_sq)
if (moved_piece.type == KING)
{
    return 2 + destination_index(KING, from_sq, to_sq)
}

num_same_pieces_before = popcount(pos.piecesBB(moved_piece) & before_bb(from_sq))
local_offset = max_moves_on_empty_board(moved_piece.type) * num_same_pieces_before
offset = 2 + 8 + localOffset

// now we have to compute the destination index and add it to the offset
if (moved_piece.type == PAWN)
{
    offset += pawn_destination_index(move, pos.side_to_move)
}
else
{
    offset += destination_index(moved_piece.type, from_sq, to_sq)
}

return offset
```

## Converting Move Index to Move

To decode we have to do the inverse. Find the value space block which corresponds to the moved piece (we can do it by repeatedly going through value space blocks and taking the first one for which `move_index - offset < max_moves_on_empty_board(pt)`) and decode the destination index.

In pseudo-code:
```
// helper function. Returns true if the index fell into the piece_type's range
index_to_move_for_piece_type(piece_type, pos, index, ref from_sq, ref to_sq, ref offset)
{
    piece = Piece(piece_type, pos.side_to_move)

    next_offset = offset + max_moves_on_empty_board(piece_type) * pos.piece_count(piece)

    // if index < next_offset then we know the encoded piece if of the piece_type type
    // because we fall into the range of value space blocks
    // assigned for this piece type for this position.
    // Otherwise it's not our job to decode it.
    if (index < nextOffset)
    {
        // compute the index of the piece of this type to which the range corresponds
        local_offset = index - offset
        n = local_offset / max_moves_on_empty_board(piece_type)
        local_offset += n * max_moves_on_empty_board(piece_type)
        piece_bb = pos.piece_bb(piece)

        // pop all square from the bb which lie before our piece
        for (i = 0; i < n; ++i)
        {
            piece_bb.pop_first_square();
        }

        // now the first bit in the pieces bb is our piece
        ref from_sq = piece_bb.first_square();
        ref to_sq = destination_square(piece_type, from_sq, local_offset)

        return true
    }

    ref offset = nextOffset;

    return false
}

// main decoding function
index_to_move(pos, index)
{
    if (index < 2 + 8)
    {
        if (index == 0) return king side castle
        if (index == 1) return queen side castle
        else
        {
            // king move, there's always only one king
            from_sq = pos.king_square(pos.side_to_move)
            to_sq = destination_square(KING, from_sq, index - 2)
            return Move(from_sq, to_sq)
        }
    }
    else
    {
        offset = 2 + 8

        {
            // similarily as for the piece in index_to_move_for_piece_type
            // but the decoding of the pawn destination index is different
            piece = Piece(PAWN, pos.side_to_move)
            next_offset = offset + max_moves_on_empty_board(PAWN) * pos.piece_count(piece)

            // pawn move
            if (index < nextOffset)
            {
                local_offset = index - offset
                n = local_offset / max_moves_on_empty_board(PAWN)
                local_offset += n * max_moves_on_empty_board(PAWN)
                piece_bb = pos.piece_bb(piece)
                for (i = 0; i < n; ++i)
                {
                    piece_bb.pop_first_square()
                }

                from_sq = piece_bb.first_square()
                return destination_index_to_pawn_move(
                    pos,
                    local_offset,
                    from_sq,
                    pos.side_to_move
                    )
            }

            offset = next_offset
        }

        // it didn't fall into the range of pawn value blocks,
        // increase offset and move to other piece types

        move = NULL;
        if (detail::index_to_move_for_piece_type(
            KNIGHT, pos, index, ref move.from_sq, ref move.to_sq, ref offset)) return move;

        if (detail::index_to_move_for_piece_type(
            BISHOP, pos, index, ref move.from_sq, ref move.to_sq, ref offset)) return move;

        if (detail::index_to_move_for_piece_type(
            ROOK, pos, index, ref move.from_sq, ref move.to_sq, ref offset)) return move;

        if (detail::index_to_move_for_piece_type(
            QUEEN, pos, index, ref move.from_sq, ref move.to_sq, ref offset)) return move;
    }

    // This should not be reachable.
    // If it's reached it means that the index was too high
    // and doesn't correspond to any piece.
    return NULL;
}
```

## Determining the size of the result

To properly encode/decode the value we have to be able to determine, based on position, how many bytes to use for each move. We cannot simply say to use 1 byte whenever the value fits in one byte.

So we use the following criteria.
By default we use 1 byte of the `move_index` to encode the move.
IF the position contains more than 2 queens for the side to move THEN we use 2 bytes for the `move_index`.

This is as such because on the starting board we have a total length of value space blocks equal to `2 + 12*8 + 8*2 + 13*2 + 14*2 + 27 + 8 == 203`.
Each knight promotion makes it smaller.
Each bishop promotion adds 1, each rook promotion adds 2, each queen promotion add 15.
We consider N queen promotions and 8-N rook promotions. This is the parametrized worst case.
Then the total amount is `2 + 8*2 + 13*2 + 14*(2+8-N) + 27*(1+N) + 8 == 219 + 13N`.
So for different `N` we have:
```
N = 0 -> 219
N = 1 -> 232
N = 2 -> 245
N = 3 -> 258 > 255 // we cannot encode this value in one byte.
```
Hence for positions with at least 3 queens we use a 16 bit index (for ease of parsing we want whole bytes).
These positions are so rare that it doesn't have a noticable impact on the compression.

## Appendix A: Lookup table initialization.

We can use lookup tables for lookup of destination index and the inverse.
One implementation of the initialization would be:
```
init()
{
    piece_types = {
        KNIGHT,
        BISHOP,
        ROOK,
        QUEEN,
        KING
    }

    for (pt in piece_types)
    {
        for (sq in a1 ... h8)
        {
            destination_bb[pt][sq] = attacks_bb(pt, sq, EMPTY_BITBOARD);

            // iterator over set bits
            i = 0
            for (destination_sq in destination_bb[pt][sq])
            {
                destination_square[pt][sq][i] = destination_sq;
                destination_index[pt][sq][destination_sq] = i;

                ++i;
            }
        }
    }

    return tables;
}
```