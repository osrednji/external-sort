# GenerateAndSort Benchmark Solution

A high-performance, O(1) heap allocation, O(N) sorting benchmark suite designed to generate and sort massive text data sets (supporting up to 100 GB+). 

The solution uses a high-performance external parallel radix sort pipeline with LZ4 multi-pass compression to sort records of `[Integer]. [String]` format case-insensitively.

---

## 1. How to Generate Test Files

The `FileGenerator` tool generates deterministic test data using a specified seed and writes at maximum throughput using a 64 MB buffered sequential file stream.

### Running with Defaults
To run the generator using settings defined in `FileGenerator/appsettings.json`:
```bash
dotnet run --project FileGenerator
```

### Running with Custom Size
To generate a file of a specific size (in MB) using the default path:
```bash
# Generates a 100 MB file
dotnet run --project FileGenerator -- 100
```

### Running with Custom Path, Size, and Seed
```bash
# Format: dotnet run --project FileGenerator -- [OutputPath] [SizeInMB] [Seed]
dotnet run --project FileGenerator -- "C:/Data/input/input.txt" 5000 1234
```

---

## 2. How to Run the Benchmark

The `Benchmark` tool reads the generated data, runs the external sorting algorithm, and produces an execution report.

### Running the Benchmark
To run the benchmark using configuration parameters defined in `Benchmark/appsettings.json`:
```bash
dotnet run --project Benchmark
```

### Running with Custom Arguments
You can override settings by passing arguments to the benchmark:
```bash
# Format: dotnet run --project Benchmark -- [InputPath] [RunsPerAlgo] [WorkDir] [ResultPath]
dotnet run --project Benchmark -- "../input/input.txt" 5 "../output" "../result.txt"
```

---

## 3. Storage & Output Locations

### Generated Input Files
* **Default Location**: `Benchmark/input/input.txt`
* Paths configured in `FileGenerator/appsettings.json` are resolved relative to the compiled executable directory (`FileGenerator/bin/...`), but you can specify absolute paths. The directory is created automatically if it does not exist.

### Intermediate Sorting Files
* **Default Location**: `Benchmark/output/` (contains temporary workspaces created during execution).
* Temporary directories like `_tmp_...` are automatically cleaned up immediately after runs to save disk space.

### Final Sorted Output Files
* **Default Location**: `Benchmark/output/SortingAlgorithm_run01.txt`
* In contrast to intermediate files, the final sorted outputs are **preserved** in the output directory for inspection and validation.

### Final Benchmark Report
* **Default Location**: `Benchmark/benchmark_result.txt`
* Contains detailed performance stats, run-by-run metrics, and summary tables.

---

## 4. Configuration Reference

Both projects are fully configuration-driven via standard JSON files.

### `FileGenerator/appsettings.json`
Located in the `FileGenerator` project root:
```json
{
  "DefaultPath": "../../../../Benchmark/input/input.txt", // Target file to generate
  "SizeMb": 10,                                            // Default size in megabytes
  "Seed": 42,                                             // Deterministic PRNG seed
  "Strings": [                                            // Dictionary of words used for line generation
    "Apple",
    "Banana is yellow",
    ...
  ]
}
```

### `Benchmark/appsettings.json`
Located in the `Benchmark` project root:
```json
{
  "InputPath": "../../../input/input.txt",                 // Source input file to sort
  "Runs": 1,                                               // Number of benchmark iterations
  "WorkDir": "../../../output",                            // Folder for intermediate work & sorted outputs
  "ResultPath": "../../../benchmark_result.txt"            // Destination of the final benchmark report
}
```

---

## 5. Architecture Details
* **Radix Sort & Rank-Mapping**: Rather than comparing strings directly, the algorithm maps records using high-performance parallel radix sorting on the integer keys and string prefixes, preserving stability.
* **LZ4 Compression**: Multi-pass external sorting uses LZ4 compression during intermediate merges to minimize disk I/O bottlenecks.
* **Zero-Allocations**: Rents all large buffers from static pools configured in `BenchmarkPools.cs` to eliminate garbage collector overhead.
