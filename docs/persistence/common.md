#Header storage

Each database can have it's own format for storing headers, but there is one common one - described here.

Some tags and other values from each pgn game imported are stored in a packed manner in the \_header file (can be one for each game level, depends on the format used):

- 4B game index
- 2B total size
- 1B game result
- 4B date

    - 2B year // 0 indicates unknown
    - 1B month // 0 indicates unknown
    - 1B day // 0 indicates unknown

- 2B ECO code
- 2B ply count // 2^16-1 indicates unknown
- up to 768B of packed strings (variable length)
    - each string is preceded by it's length stored in one byte.
      maximum single string length is 255
    - event
    - white
    - black

\_index files store 8B offsets into the \_header file. A value in position i of the \_index file is the offset of the entry in \_header file of the ith game. (Alpha format uses indirect access through index, beta format uses direct access through offsets for example)


#Manifest

Manifest (file manifest) stores information that can identify the database type used and is used for some verification.

Structure:

- 1B key length N
- N bytes of key
- 16B of endianness signature - database format can specify whether it needs matching endiannes between creation and opening (for example if the database files were created on a different system). The signature can be found in src/util/EndiannessSignature.h

The key is unique for each database format. It can be used to dynamically instantiate right database.

#Stats

File stats stores the overall statistics for the database. It is defined by the base database class and is not a subject for modification for new formats.

The file contains 6 integers in text form delimited by whitespace characters representing in order:

- number of human games
- number of total positions in human games
- number of engine games
- number of total positions in engine games
- number of server games
- number of total positions in server games
