using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

/// <summary>
/// Entry point. Wires up the three algorithms and runs the benchmark.
///
/// Usage:
///   BenchmarkRunner &lt;input_file&gt; [runs_per_algorithm] [work_dir] [result_file]
///
/// Examples:
///   BenchmarkRunner sorted_input.txt
///   BenchmarkRunner sorted_input.txt 5
///   BenchmarkRunner sorted_input.txt 10 ./bench_work ./results.txt
///
/// Defaults:
///   runs_per_algorithm = 10
///   work_dir           = ./bench_work  (created automatically, cleaned after each run)
///   result_file        = ./benchmark_result.txt
/// </summary>
class Program
{
    static async Task<int> Main(string[] args)
    {
        string inputPath  = args.Length >= 1 ? args[0] : @"C:\Users\HP\Downloads\New folder\input_1gb.txt";
        int    runs       = args.Length >= 2 ? int.Parse(args[1]) : 1;
        string workDir    = args.Length >= 3 ? args[2] : "./bench_work";
        string resultPath = args.Length >= 4 ? args[3] : "./benchmark_result.txt";

        if (args.Length < 1)
        {
            Console.WriteLine("No custom input file provided.");
            Console.WriteLine($"Defaulting to input file : {inputPath}");
            Console.WriteLine($"Defaulting to runs/algo  : {runs}");
            Console.WriteLine($"Defaulting to work dir   : {workDir}");
            Console.WriteLine($"Defaulting to report path: {resultPath}");
            Console.WriteLine();
        }

        if (!File.Exists(inputPath))
        {
            Console.Error.WriteLine($"ERROR: Input file not found: {inputPath}");
            return 1;
        }

        if (runs < 1)
        {
            Console.Error.WriteLine("ERROR: runs_per_algorithm must be >= 1");
            return 1;
        }

        // ── Register algorithms in the order they will run ────────────────────
        var algorithms = new List<ISortAlgorithm>
        {
            //new BaselineSortAlgorithm(),        // V1 — baseline
            //new PipelineDeflateSortAlgorithm(), // V2 — pipeline + Deflate
            //new OptimizedPipelineDeflateSortAlgorithm(), // V2 Optimized — zero-allocation pipeline + Deflate
            //new RankPackingDeflateSortAlgorithm(), // V4 Rank-Packed Pipeline+Deflate
            //new Lz4ParallelSortAlgorithm(),     // V3 — LZ4 + parallel sort
            new OptimizedLz4ParallelSortAlgorithm(),     // V3 Optimized — zero-allocation LZ4 + Parallel Merge Tree
            new OptimizedLz4RankSortAlgorithm(),   // V4 Rank-Packed LZ4 + Parallel Merge Tree
            new ZeroAllocLz4ByteSortAlgorithm(),   // V5 Extreme Zero-Allocation ByteSort
            new ZeroAllocRadixByteSortAlgorithm(),   // V6 Zero-Allocation Radix-Pipeline + Byte-Level Merge
            new ZeroAllocPoolRadixByteSortAlgorithm()   // V7 Zero-Allocation Pool-Pipeline + Radix + Byte-Level Merge
        };

        // ── Verify correctness of V7 ──────────────────────────────────────────
        try
        {
            string output = Path.Combine(workDir, "v7_sorted_verify.txt");
            if (File.Exists(output)) File.Delete(output);

            Console.WriteLine("=== Correctness Verification for V7 ===");
            Console.WriteLine("Running V7 Sort...");
            var sorter = new ZeroAllocPoolRadixByteSortAlgorithm();
            var sw = System.Diagnostics.Stopwatch.StartNew();
            await sorter.SortAsync(inputPath, output);
            sw.Stop();
            Console.WriteLine($"V7 Sort completed in {sw.Elapsed:mm\\:ss\\.fff}");

            Console.WriteLine("Verifying sorted output...");
            using var fs = new FileStream(output, FileMode.Open, FileAccess.Read, FileShare.Read, 1024 * 1024);
            using var sr = new StreamReader(fs, Encoding.UTF8);

            string lastKey = "";
            int lastNum = -1;
            long lineCount = 0;
            bool success = true;

            string? line;
            while ((line = await sr.ReadLineAsync()) != null)
            {
                lineCount++;
                int dot = line.IndexOf('.');
                if (dot == -1)
                {
                    Console.WriteLine($"Error at line {lineCount}: No dot found in '{line}'");
                    success = false;
                    break;
                }
                int num = int.Parse(line.Substring(0, dot));
                string key = line.Substring(dot + 1).TrimStart();

                if (lineCount > 1)
                {
                    int cmp = string.Compare(key, lastKey, StringComparison.OrdinalIgnoreCase);
                    if (cmp < 0)
                    {
                        Console.WriteLine($"Error at line {lineCount}: Key out of order!");
                        Console.WriteLine($"  Prev: {lastNum}. {lastKey}");
                        Console.WriteLine($"  Curr: {num}. {key}");
                        success = false;
                        break;
                    }
                    else if (cmp == 0)
                    {
                        if (num < lastNum)
                        {
                            Console.WriteLine($"Error at line {lineCount}: Number out of order for identical key '{key}'!");
                            Console.WriteLine($"  Prev: {lastNum}");
                            Console.WriteLine($"  Curr: {num}");
                            success = false;
                            break;
                        }
                    }
                }

                lastKey = key;
                lastNum = num;
            }

            // Dispose streams explicitly so we don't hit sharing violations
            sr.Dispose();
            fs.Dispose();

            if (File.Exists(output))
            {
                File.Delete(output);
            }

            if (success)
            {
                Console.WriteLine($"Verification SUCCESSFUL! Verified {lineCount} lines.");
            }
            else
            {
                Console.WriteLine("Verification FAILED!");
                return 3;
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"FATAL ERROR DURING VERIFICATION: {ex}");
            return 2;
        }

        // ── Run the benchmark ──────────────────────────────────────────────────
        try
        {
            await BenchmarkRunner.RunAsync(algorithms, inputPath, workDir, resultPath, runs);
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"FATAL ERROR DURING BENCHMARK: {ex}");
            return 4;
        }
    }
}
