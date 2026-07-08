using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// ── Shared types ─────────────────────────────────────────────────────────────

/// <summary>Contract every algorithm must implement.</summary>
public interface ISortAlgorithm
{
    string Name        { get; }
    string Description { get; }
    Task SortAsync(string inputPath, string outputPath);
}

/// <summary>One timed run result.</summary>
public record RunResult(string AlgorithmName, int RunNumber, TimeSpan Elapsed, bool Success, string? Error = null);

// ── Benchmark framework ───────────────────────────────────────────────────────

public static class BenchmarkRunner
{
    /// <summary>
    /// Runs each algorithm <paramref name="runsPerAlgorithm"/> times sequentially,
    /// computes the median elapsed time for each, and writes a result report.
    /// </summary>
    /// <param name="algorithms">Algorithms to benchmark, run in order.</param>
    /// <param name="inputPath">Source file (read-only, shared across all runs).</param>
    /// <param name="workDir">Directory for per-run output files (created if absent).</param>
    /// <param name="resultPath">Path for the final benchmark report.</param>
    /// <param name="runsPerAlgorithm">How many timed runs per algorithm (default 10).</param>
    public static async Task RunAsync(
        IReadOnlyList<ISortAlgorithm> algorithms,
        string inputPath,
        string workDir,
        string resultPath,
        int runsPerAlgorithm = 10)
    {
        if (runsPerAlgorithm < 1) throw new ArgumentOutOfRangeException(nameof(runsPerAlgorithm));

        Directory.CreateDirectory(workDir);

        var allResults = new List<RunResult>();
        int totalRuns  = algorithms.Count * runsPerAlgorithm;
        int runsDone   = 0;

        PrintHeader(algorithms, runsPerAlgorithm, inputPath);

        // ── Main loop: algorithm by algorithm ────────────────────────────────
        foreach (var algo in algorithms)
        {
            Console.WriteLine();
            Console.WriteLine($"┌─ {algo.Name} ─────────────────────────────────────────");
            Console.WriteLine($"│  {algo.Description}");
            Console.WriteLine($"│  Runs: {runsPerAlgorithm}");
            Console.WriteLine("│");

            for (int run = 1; run <= runsPerAlgorithm; run++)
            {
                string outputPath = Path.Combine(workDir, $"{SanitiseName(algo.Name)}_run{run:D2}.txt");

                // Delete stale output from a previous benchmark session
                if (File.Exists(outputPath)) File.Delete(outputPath);

                Console.Write($"│  Run {run,2}/{runsPerAlgorithm} ... ");

                RunResult result;
                try
                {
                    var sw = Stopwatch.StartNew();
                    await algo.SortAsync(inputPath, outputPath);
                    sw.Stop();

                    result = new RunResult(algo.Name, run, sw.Elapsed, Success: true);
                    Console.WriteLine($"{sw.Elapsed:mm\\:ss\\.fff}");
                }
                catch (Exception ex)
                {
                    result = new RunResult(algo.Name, run, TimeSpan.Zero, Success: false, Error: ex.Message);
                    Console.WriteLine($"FAILED — {ex.Message}");
                }

                allResults.Add(result);
                runsDone++;

                // Clean up output after each run to avoid filling the disk
                if (File.Exists(outputPath))
                    try { File.Delete(outputPath); } catch { }

                PrintProgress(runsDone, totalRuns);
            }

            // Per-algorithm summary
            var algoResults = allResults.Where(r => r.AlgorithmName == algo.Name).ToList();
            var median      = ComputeMedian(algoResults);
            var min         = algoResults.Where(r => r.Success).Select(r => r.Elapsed).DefaultIfEmpty().Min();
            var max         = algoResults.Where(r => r.Success).Select(r => r.Elapsed).DefaultIfEmpty().Max();
            int failed      = algoResults.Count(r => !r.Success);

            Console.WriteLine("│");
            Console.WriteLine($"│  Median : {FormatTs(median)}");
            Console.WriteLine($"│  Min    : {FormatTs(min)}   Max: {FormatTs(max)}");
            if (failed > 0)
                Console.WriteLine($"│  Failed : {failed} run(s)");
            Console.WriteLine("└──────────────────────────────────────────────────────");
        }

        // ── Build & write report ──────────────────────────────────────────────
        string report = BuildReport(algorithms, allResults, runsPerAlgorithm, inputPath);
        await File.WriteAllTextAsync(resultPath, report, Encoding.UTF8);

        Console.WriteLine();
        Console.WriteLine("══════════════════════════════════════════════════════");
        Console.WriteLine(" BENCHMARK COMPLETE");
        Console.WriteLine("══════════════════════════════════════════════════════");
        Console.WriteLine(ExtractSummarySection(report));
        Console.WriteLine($"Full report written to: {resultPath}");
    }

    // ── Statistics ────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the median elapsed time for successful runs of one algorithm.
    /// Uses the lower median for even counts (standard practice).
    /// Returns TimeSpan.Zero if there are no successful runs.
    /// </summary>
    public static TimeSpan ComputeMedian(IEnumerable<RunResult> results)
    {
        var sorted = results
            .Where(r => r.Success)
            .Select(r => r.Elapsed)
            .OrderBy(t => t)
            .ToList();

        if (sorted.Count == 0) return TimeSpan.Zero;

        int mid = sorted.Count / 2;
        // Even count → lower median (avoids averaging two TimeSpans, keeps it simple)
        return sorted.Count % 2 == 1 ? sorted[mid] : sorted[mid - 1];
    }

    // ── Report builder ────────────────────────────────────────────────────────

    private static string BuildReport(
        IReadOnlyList<ISortAlgorithm> algorithms,
        List<RunResult>               allResults,
        int                           runsPerAlgorithm,
        string                        inputPath)
    {
        var sb = new StringBuilder();

        sb.AppendLine("╔══════════════════════════════════════════════════════════════╗");
        sb.AppendLine("║              SORT ALGORITHM BENCHMARK REPORT                ║");
        sb.AppendLine("╚══════════════════════════════════════════════════════════════╝");
        sb.AppendLine();
        sb.AppendLine($"Date       : {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine($"Input file : {inputPath}");
        try { sb.AppendLine($"File size  : {new FileInfo(inputPath).Length / (1024.0 * 1024.0):F1} MB"); }
        catch { sb.AppendLine("File size  : (unavailable)"); }
        sb.AppendLine($"Runs/algo  : {runsPerAlgorithm}");
        sb.AppendLine($"CPU cores  : {Environment.ProcessorCount}");
        sb.AppendLine($".NET       : {Environment.Version}");
        sb.AppendLine();

        // ── Per-algorithm detail ──────────────────────────────────────────────
        sb.AppendLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        sb.AppendLine("  PER-ALGORITHM RESULTS");
        sb.AppendLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        var medians = new Dictionary<string, TimeSpan>();

        foreach (var algo in algorithms)
        {
            var runs   = allResults.Where(r => r.AlgorithmName == algo.Name).ToList();
            var median = ComputeMedian(runs);
            var min    = runs.Where(r => r.Success).Select(r => r.Elapsed).DefaultIfEmpty().Min();
            var max    = runs.Where(r => r.Success).Select(r => r.Elapsed).DefaultIfEmpty().Max();
            int ok     = runs.Count(r => r.Success);
            int failed = runs.Count(r => !r.Success);

            medians[algo.Name] = median;

            sb.AppendLine();
            sb.AppendLine($"  Algorithm : {algo.Name}");
            sb.AppendLine($"  Desc      : {algo.Description}");
            sb.AppendLine($"  Successful: {ok}/{runsPerAlgorithm}");
            sb.AppendLine($"  Median    : {FormatTs(median)}");
            sb.AppendLine($"  Min       : {FormatTs(min)}");
            sb.AppendLine($"  Max       : {FormatTs(max)}");

            if (ok > 1)
            {
                var times = runs.Where(r => r.Success).Select(r => r.Elapsed.TotalMilliseconds).ToList();
                double avg    = times.Average();
                double stdDev = Math.Sqrt(times.Select(t => Math.Pow(t - avg, 2)).Average());
                sb.AppendLine($"  Avg       : {TimeSpan.FromMilliseconds(avg):mm\\:ss\\.fff}");
                sb.AppendLine($"  StdDev    : {stdDev / 1000.0:F3}s");
            }

            sb.AppendLine();
            sb.AppendLine("  Run-by-run:");
            foreach (var r in runs)
            {
                string marker = r.Success ? FormatTs(r.Elapsed) : $"FAILED ({r.Error})";
                sb.AppendLine($"    Run {r.RunNumber,2}: {marker}");
            }
        }

        // ── Summary table ─────────────────────────────────────────────────────
        sb.AppendLine();
        sb.AppendLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        sb.AppendLine("  SUMMARY  (sorted by median, fastest first)");
        sb.AppendLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        sb.AppendLine();

        var ranked = medians
            .Where(kv => kv.Value > TimeSpan.Zero)
            .OrderBy(kv => kv.Value)
            .ToList();

        if (ranked.Count == 0)
        {
            sb.AppendLine("  No successful runs recorded.");
        }
        else
        {
            var fastest        = ranked[0];
            int nameColWidth   = ranked.Max(kv => kv.Key.Length) + 2;

            sb.AppendLine($"  {"Algorithm".PadRight(nameColWidth)}  {"Median",12}  {"vs Fastest",12}  {"vs Previous",12}");
            sb.AppendLine($"  {new string('-', nameColWidth)}  {"----------",12}  {"----------",12}  {"-----------",12}");

            TimeSpan? prev = null;
            for (int i = 0; i < ranked.Count; i++)
            {
                var (name, median) = (ranked[i].Key, ranked[i].Value);
                string vsFastest = i == 0 ? "  --  (best)" : $"+{(median - fastest.Value).TotalSeconds:F2}s ({median.TotalMilliseconds / fastest.Value.TotalMilliseconds:F2}x slower)";
                string vsPrev    = prev == null ? "  --" : $"+{(median - prev.Value).TotalSeconds:F2}s";
                string tag       = i == 0 ? " ◄ FASTEST" : "";

                sb.AppendLine($"  {name.PadRight(nameColWidth)}  {FormatTs(median),12}  {vsFastest,-24}  {vsPrev,-14}{tag}");
                prev = median;
            }

            sb.AppendLine();
            sb.AppendLine($"  ★  FASTEST: {fastest.Key}  (median {FormatTs(fastest.Value)})");

            // Speedup narrative
            if (ranked.Count > 1)
            {
                var slowest = ranked[^1];
                double speedup = slowest.Value.TotalMilliseconds / fastest.Value.TotalMilliseconds;
                sb.AppendLine($"  ★  Fastest is {speedup:F2}x faster than slowest ({slowest.Key})");
            }
        }

        sb.AppendLine();
        sb.AppendLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        sb.AppendLine("  END OF REPORT");
        sb.AppendLine("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        return sb.ToString();
    }

    // ── Console helpers ───────────────────────────────────────────────────────

    private static void PrintHeader(IReadOnlyList<ISortAlgorithm> algorithms, int runs, string input)
    {
        Console.WriteLine("══════════════════════════════════════════════════════");
        Console.WriteLine(" SORT ALGORITHM BENCHMARK");
        Console.WriteLine("══════════════════════════════════════════════════════");
        Console.WriteLine($" Input      : {input}");
        Console.WriteLine($" Algorithms : {algorithms.Count}");
        Console.WriteLine($" Runs/algo  : {runs}");
        Console.WriteLine($" Total runs : {algorithms.Count * runs}");
        Console.WriteLine($" CPU cores  : {Environment.ProcessorCount}");
        Console.WriteLine("══════════════════════════════════════════════════════");
    }

    private static void PrintProgress(int done, int total)
    {
        int width   = 40;
        int filled  = (int)Math.Round((double)done / total * width);
        string bar  = new string('█', filled) + new string('░', width - filled);
        Console.Write($"\r│  [{bar}] {done}/{total}  ");
        if (done == total) Console.WriteLine();
    }

    private static string ExtractSummarySection(string report)
    {
        int idx = report.IndexOf("SUMMARY", StringComparison.Ordinal);
        return idx < 0 ? report : report[idx..];
    }

    private static string FormatTs(TimeSpan ts) =>
        ts == TimeSpan.Zero ? "  --:--   " : ts.ToString(@"mm\:ss\.fff");

    private static string SanitiseName(string name) =>
        string.Concat(name.Select(c => char.IsLetterOrDigit(c) ? c : '_'));
}
