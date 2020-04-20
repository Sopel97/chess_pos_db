# Compressed Move Encoding Scheme

## Definitions

Each compressed move takes exactly 2 bytes.

Square are encoded in little-endian rank-file mapping.
I.e. in order a1, b1, c1, ..., a2, b2, c2, ..., f8, g8, h8

## Scheme

```
- First byte. From most significant bits.
    - move_type             : 2 bits
        0=normal, 1=promotion, 2=castling, 3=en passant
    - from_square           : 6 bits
        This is king square for castling move
- Second byte. From most significant bits.
    - to_square             : 6 bits
        Castling are encoded as capturing the rook with which the king castles
    - promoted_piece_type   : 2 bits
        0=knight, 1=bishop, 2=rook, 3=queen
        If the move is not a promotion this value has to be 0.
```