# BCGN format specification

BCGN is a binary format for storing chess games.
It can store various tags and movetext.
All tags are optional but some have to be encoded even if empty.

## Basic encoding scheme

All multibyte values are encoded in big-endian byte order.
All string are encoded as fields of varying size with the following scheme:
```
- length N     : 1 bytes
- utf-8 text   : N bytes
```
The string is NOT null terminated. Strings can be of length 0.
The size being encoded as a 1 byte integer limits the size of the string to 255 bytes at most.

## File Header Specification

File header always has 32 bytes and has to be at the beginning of a BCGN file.
```
- "BCGN"               : 4 bytes
- version              : 1 byte
    - 0
- compression_scheme   : 1 byte
    - 0 = 2 bytes per move, use CompressedMove. See compressed_move.md for the encoding scheme.
    - 1 = 1-2 bytes per move (almost always 1), use move index. See move_index.md for the encoding scheme.
    - 2 = variable amount of bits per move. See variable_length.md for details.
- aux_compression      : 1 byte
    - 0 = none
- flags                : 1 byte
    - headerless        : 1 bit
        If headerless then game header includes only total_length,
        ply_count, result, flags, custom_start_pos (if set in flags).
    - *RESERVED*        : 7 bits
- *RESERVED*           : 24 bytes
- TOTAL                : 32 bytes
```

## Game Entry Specification

Each game in the file must have the same scheme (which is specified in the file header).
We define the following addresses within the file:

BASE_GAME_ADDRESS[i] - the address of the first byte of the i-th game entry
BASE_MOVETEXT_ADDRESS[i] - the address of the first byte of the i-th game movetext

Each entry starts with the header and is immediately followed by the movetext.
After the movetext either the next game follows immediately or the end of file is reached.

### Game Header Specification

If headerless flag IS NOT set:
```
BASE_GAME_ADDRESS[i]:
- total_length                    : 2 bytes
      total length of this game header + movetext entry in bytes
      i.e. BASE_GAME_ADDRESS[i] + total_length == BASE_GAME_ADDRESS[i+1]
- header_length                   : 2 bytes
      total length of the header in bytes
      i.e. BASE_GAME_ADDRESS[i] + total_header_length == BASE_MOVETEXT_ADDRESS[i]
- combined ply count and result   : 2 bytes
    - ply_count                    : 14 highest bits
    - result                       : 2 lowest bits
        0 unknown, 1 white win, 2 black win, 3 draw
- date                            : 4 bytes
    - year                         : 2 bytes
    - month                        : 1 byte
    - day                          : 1 byte
        NOTE: Any part is allowed to have value 0, meaning unknown
- white_elo                       : 2 bytes
- black_elo                       : 2 bytes
- round                           : 2 bytes
- eco                             : 2 bytes
    - [ABCDE]                      : 1 byte
    - index                        : 1 byte
- flags                           : 1 byte
    Going from most significant bit to least significant bit:
    - *RESERVED*                   : 6 bits
    - has_custom_start_position    : 1 bit
    - has_additional_tags          : 1 bit

- SUBTOTAL                        : 19 bytes
Now follow mixed length or optional fields.

If flags[has_custom_start_position]:
    - start_position              : 24 bytes
        For encoding see compressed_position.md

- white_player                    : string
- black_player                    : string
- event                           : string
- site                            : string

If flags[has_additional_tags]:
    - num_additional_tags K       : 1 byte
    (
        - name                    : string
        - value                   : string
    )*K
```

If headerless fla IS set:
```
BASE_GAME_ADDRESS[i]:
- total_length                    : 2 bytes
      total length of this game header + movetext entry in bytes
      i.e. BASE_GAME_ADDRESS[i] + total_length == BASE_GAME_ADDRESS[i+1]
- combined ply count and result   : 2 bytes
    - ply_count                    : 14 highest bits
    - result                       : 2 lowest bits
        0 unknown, 1 white win, 2 black win, 3 draw
- flags                           : 1 byte
    Going from most significant bit to least significant bit:
    - *RESERVED*                   : 6 bits
    - has_custom_start_position    : 1 bit
    - has_additional_tags          : 1 bit
        This flag is ignored.

- SUBTOTAL                        : 5 bytes
Now follow mixed length or optional fields.

If flags[has_custom_start_position]:
    - start_position              : 24 bytes
        For encoding see compressed_position.md
```

### Game Movetext Specification

Movetext encoding depends on file level compression_scheme.
For the schemes see move_index.md and compressed_move.md
```
BASE_MOVETEXT_ADDRESS[i]:
(
    - encoded_move                : length depends on scheme
)*ply_count
```
