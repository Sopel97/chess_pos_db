# chess_pos_db

chess_pos_db is a free, opensource software aiming to provide a high performance database service for aggregation of chess position data from chess games. It provides a simple TCP interface for interprocess communication, a console interface, and an optional windows GUI. The goal is to achieve cutting-edge performance and unmatched possibilities.

For a Windows GUI see [HERE](https://github.com/Sopel97/chess_pos_db_gui). It also contains setup instructions.

Notable features:

- 3 data formats allowing different tradeoff between space and information stored.

- Creation of a position database from PGN or BCGN files. Data being agreggated (depends on database format chosen):

    - Win count from a given position
    - Draw count from a given position
    - Loss count from a given position
    - Total elo difference for players that reach a given position (average = total / game_count)
    - Some PGN Tags (result, event, white, black, plies, ECO) of the first (depending on database format also last) game with this position.

- High performance and little storage

    - On modern hardware and fast storage it can import between 4 to 10 million positions per second depending on the database format chosen.
    - Each unique position entry takes between 16 to 32 bytes depending on the format used. For typical large dataset only about 70% or less of the positions are unique.
    - Querying is optimized for minimal number of disk seeks. For example for the db_beta format querying all data for a single move and all possible moves takes \~1 second an HDD and is blazingly fast on an SSD.
    - Index kept in RAM, uses orders of magnitude times less space than the database and accelerates the queries (size globally configurable).

- High limits

    - Can handle trillions of positions
    - Up to 4 billion games (can be increased in the future, and some formats may work with higher numbers)
    - No limit on input/output file sizes (can handle large PGN files). The only limit is from the filesystem.

- Distinction between continuations (exact move played to arrive at this position) and transpositions (different move played to arrive at this position).
- Local, file based database structure allowing for easy archiving, copying, and distribution.
- Extensive configuration. (see cfg/config.json)
- Console user interface
- A simple TCP server allowing managing and quering databases.

Notable codebase features:

- High performance streaming PGN parser with varying degree of validation
- SAN move parser with varying degree of validation
- Support for BCGN - a more space efficient alternative to PGN.
- Clean chess abstraction. Parts can be used as a chess library.
- External algorithms with support for async IO through worker threads.
- Various integer compression schemes

# Building
Currently only Windows is explicitly supported. Though the code is mostly standard compliant C++17 - there are only a few instances of non-portable code, most notably 64 bit file seek functions in External module.
Not tested on systems other that Windows.

Requires 64-bit builds to work as intended. May not compile or have bugs on 32-bit builds.

Compiles with Visual Studio 2019 MSVC Compiler (.sln included).

Support for other compilers and other operating systems is planned but there is no definitive deadline.

# Dependencies

Licenses specified in header files or in respective folders.

- libcppjson - included in /lib folder
- xxhash - included in /lib folder
- googletest - vcpkg
- brynet - vcpkg

# UI
For a Windows GUI see [HERE](https://github.com/Sopel97/chess_pos_db_gui). It also contains setup instructions.

It's a command line application with 3 modes of operation:

1. When launched without any command line arguments

    - Provides basic console interface functionality for creating/opening/querying/closing/destroying databases. See `help` command.
    - Currently supports only one hardcoded database format.

2. When launched with 2 parameters `<db_path> <port>`

    - Opens a database under `<db_path>`
    - Creates a TCP server listening on `<port>` and accepts query requests (see docs for specification).
    - Responses are sent back through the same TCP connection.

3. When launched with 1 parameter `<port>`

    - Creates a TCP server listening on `<port>` and accepts various commands (see docs). For example:

        - creating a database
        - merging a database
        - opening a database
        - querying

    - The interface is stateful - only one database can exist at once. Currently only one simultanuous connection is supported.
    - Not meant for remote use. Can function as an interprogram messaging protocol.

