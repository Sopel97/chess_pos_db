# Message layout

- 4 bytes of payload length N in bytes, stored in little endian (least significant byte first)
- 4 bytes of N xored with 3173045653 (0xBD20D595) in the same byte order

    - this can be used for packet verification

- N bytes of the payload