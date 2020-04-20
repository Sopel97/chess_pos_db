# Comparison of PGN vs BCGN parsing times

The PGN implementation is insanely fast and cuts corners around the bogus specification to achieve that. It it is not strictly conforming.

BCGN supports multiple compression levels.
c0 and c1 correspond to CompressionLevel_0 and CompressionLevel_1 respectively.

The PGN file used is stripped of all comments and annotations.
Only movetext is parsed and positions in each game are iterated through.
The speed of Position::doMove alone is around 35M/s.

Two warmup runs are executed. File is buffered in RAM.
Then 3 runs are made and the average is taken.
All files have the same 1 457 174 games and 100 774 756 positions.
The file used is stripped lichess_db_standard_rated_2015-01 with <5 ply games removed.

|Format|Size [MB]|Time [s]|Games/s|Positions/s|Throughput [MB/s]|Speedup|
|-|-|-|-|-|-|-|
|.pgn|1 157|13.159|109 178|7 543 642|86.621|1|
|.bcgn c0|331|4.716|309 004|21 370 037|70.266|2.83|
|.bcgn c1|232|8.171|178 352|12 332 553|28.396|1.63|