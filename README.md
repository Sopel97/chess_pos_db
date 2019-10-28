# chess_pos_db

chess_pos_db is an opensource software aiming to provide a high performance database service for aggregation of chess position data. The goal is easy to use and integrate interprocess communication protocol, high creation and querying performance, and minimal storage overhead.

For a Windows GUI see [HERE](https://github.com/Sopel97/chess_pos_db_gui). It also contains setup instructions.

Notable features:

- Creation of a position database from pgn files. Aggregated data:

    - Win count from a given position
    - Draw count from a given position
    - Loss count from a given position
    - Some PGN Tags (result, event, white, black, plies, ECO) of the first (depending on database format also last) game with this position.

- High performance and little storage

    - On modern hardware and fast storage it can process about 10 million positions (when creating the database) in a sequential mode. (Parallel mode coming in the future).
    - Depending on a format used each position can require little or less than 20 bytes, all that while providing above statistics. Notably db_beta format requires \~17 bytes per position when there is \~6 billion of them.
    - Querying is optimized for minimal number of disk seeks. For example for the db_beta format querying all data for a single move and all possible moves takes \~1 second an HDD and is blazingly fast on an SSD.
    - Index kept in RAM, uses 500 times less space than the database and accelerates the queries.

- Distinction between continuations (exact move played to arrive at this position) and transpositions (different move played to arrive at this position).
- Local, file based database structure allowing for easy copying.
- Extensive configuration. (see cfg/config.json)
- Console user interface
- A simple TCP server aiming for interprocess communication.

Notable codebase features:

- High performance streaming PGN parser with varying degree of validation
- SAN move parser with varying degree of validation
- Clean chess abstraction.
- External algorithms with support for async IO through worker threads.
- Various integer compression schemes

# Building
Currently only Windows is explicitly supported. Though the code is mostly standard compliant C++17 - there are only a few instances of non-portable code, most notably 64 bit file seek functions in External module.
Not tested on systems other that Windows.

Compiles with Visual Studio 2019 MSVC Compiler (.sln included).

Support for other compilers and other operating systems is planned but there is no definitive deadline.

# Dependencies

Licenses specified in header files or in respective folders.

- libcppjson - included in /lib folder
- infint - included in /lib folder
- robin_hood - included in /lib folder (currently unused)
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

