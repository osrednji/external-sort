External Sort — High-Performance Large-File Sorting in C#

Sorting text files that don't fit in memory (tested up to 100 GB), iterated through
multiple algorithm versions with a statistical benchmarking framework to measure
every optimization honestly.

Each line has the format <number>. <text>, sorted by text (ordinal, case-insensitive),
then by number. The interesting part isn't the sort — it's making it fast when the
data is 10–100x larger than RAM.

Results

1.6 GB input, 12 cores, .NET 8. Median of N runs per algorithm.

VersionApproachMedianvs. baselineV3LZ4 temp files + parallel merge tree1:31.9—V4Rank-packed long keys, primitive merge tree0:50.91.8x fasterV5Zero-allocation single-pass byte parsing0:39.12.3x fasterV6Radix sort on bit-packed ulong keys, byte-level merge0:32.32.8x fasterV7V6 + full ArrayPool buffer recycling0:34.22.7x faster

Full report: benchmark_result.txt. Note V7's pooling was
slower than V6 on this workload — measured, kept, and documented rather than assumed.

Key techniques


Async pipeline overlapping read / sort / write phases via bounded Channel<T>,
so I/O and CPU work never wait on each other
Zero-allocation parsing — single-pass byte-buffer line parsing, no intermediate
strings on the hot path
Bit-packed sort keys — rank + number + index packed into a single ulong,
enabling radix sort on primitives instead of comparison sort on objects
Byte-level k-way merge — merging run files without materializing strings
LZ4 compression on temp run files to trade cheap CPU for expensive disk I/O
ArrayPool<T> buffer recycling to keep GC pressure flat under sustained load
Aggressive inlining on comparers and parsers (MethodImplOptions.AggressiveInlining)


Benchmark framework

BenchmarkRunner.cs runs each algorithm N times, verifies output correctness,
and reports median / min / max with run-by-run detail — because a single run
is an anecdote, not a measurement.

BenchmarkRunner <input_file> [runs_per_algorithm] [work_dir] [result_file]

Project layout


SortAlgorithms.cs — all algorithm versions, evolving from baseline external
merge sort to the zero-allocation radix pipeline
BenchmarkRunner.cs — statistical benchmark harness with correctness verification
FileSorterNonOptimized.cs, FileSorterOlder.cs — earlier standalone versions,
kept for the before/after story
Program.cs — entry point
