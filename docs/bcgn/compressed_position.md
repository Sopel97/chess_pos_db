# Compressed Position Encoding Scheme

## Definitions

A compressed position takes exactly 24 bytes.

A Bitboard is a 64 bit unsigned integer with bit indices corresponding to
little-endian rank-file square mapping.
I.e. the least significant bit corresponds to a1, most significant bit corresponds to h8
and the order is a1, b1, c1, ...

## Piece Encoding

An encoded Piece takes exactly 4 bits and may contain additional information:
```
Pieces without additional information:
 0 : white pawn
 1 : black pawn
 2 : white knight
 3 : black knight
 4 : white bishop
 5 : black bishop
 6 : white rook
 7 : black rook
 8 : white queen
 9 : black queen
10 : white king
11 : black king

Pieces with additional information
12 : pawn with ep square behind
    Only if en passant is possible.
    Whether it's a white or black pawn depends on the rank.
13 : white rook with coresponding castling rights (i.e. it can be involved in castling)
14 : black rook with coresponding castling rights
15 : black king and black is the side to move
```

## Scheme

```
- occupied_bb              : 8 bytes
    Stored in big-endian byte order.
    i-th bit is set iff the i-th square is occupied by a piece.
(
    - encoded_piece        : 4 bits
)*popcount(occupied_bb)
- padding (zero bits)      : (128 - popcount(occupied_bb) * 4) bits
```