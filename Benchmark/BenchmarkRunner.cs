using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Benchmark;

/// <summary>
/// Provides the execution framework for running sorting algorithm benchmarks,
/// measuring runtime performance, tracking statistics, and writing comprehensive reports.
/// </summary>
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
        var totalRuns  = algorithms.Count * runsPerAlgorithm;
        var runsDone   = 0;

        PrintHeader(algorithms, runsPerAlgorithm, inputPath);

        // ── Main loop: algorithm by algorithm ────────────────────────────────
        foreach (var algo in algorithms)
        {
            Console.WriteLine();
            Console.WriteLine(string.Format(Resources.AlgoSectionHeader, algo.Name));
            Console.WriteLine(string.Format(Resources.AlgoDescription, algo.Description));
            Console.WriteLine(string.Format(Resources.AlgoRunsCount, runsPerAlgorithm));
            Console.WriteLine(Resources.AlgoSeparator);

            for (int run = 1; run <= runsPerAlgorithm; run++)
            {
                string outputPath = Path.Combine(workDir, $"{SanitiseName(algo.Name)}_run{run:D2}.txt");

                // Delete stale output from a previous benchmark session
                if (File.Exists(outputPath)) File.Delete(outputPath);

                Console.Write(string.Format(Resources.RunProgressLine, run, runsPerAlgorithm));

                RunResult result;
                try
                {
                    var sw = Stopwatch.StartNew();
                    await algo.SortAsync(inputPath, outputPath);
                    sw.Stop();

                    result = new RunResult(algo.Name, run, sw.Elapsed, Success: true);
                    Console.WriteLine(string.Format(Resources.RunTime, sw.Elapsed));
                }
                catch (Exception ex)
                {
                    result = new RunResult(algo.Name, run, TimeSpan.Zero, Success: false, Error: ex.Message);
                    Console.WriteLine(string.Format(Resources.RunFailed, ex.Message));
                }

                allResults.Add(result);
                runsDone++;

                PrintProgress(runsDone, totalRuns);
            }

            // Per-algorithm summary
            var algoResults = allResults.Where(r => r.AlgorithmName == algo.Name).ToList();
            var median      = ComputeMedian(algoResults);
            var min         = algoResults.Where(r => r.Success).Select(r => r.Elapsed).DefaultIfEmpty().Min();
            var max         = algoResults.Where(r => r.Success).Select(r => r.Elapsed).DefaultIfEmpty().Max();
            int failed      = algoResults.Count(r => !r.Success);

            Console.WriteLine(Resources.AlgoSeparator);
            Console.WriteLine(string.Format(Resources.AlgoSummaryMedian, FormatTs(median)));
            Console.WriteLine(string.Format(Resources.AlgoSummaryMinMax, FormatTs(min), FormatTs(max)));
            if (failed > 0)
                Console.WriteLine(string.Format(Resources.AlgoSummaryFailed, failed));
            Console.WriteLine(Resources.AlgoSectionFooter);

            // Force GC collection to reclaim memory from the completed algorithm
            GC.Collect(2, GCCollectionMode.Forced, blocking: true, compacting: true);
            GC.WaitForPendingFinalizers();
        }

        // ── Build & write report ──────────────────────────────────────────────
        string report = BuildReport(algorithms, allResults, runsPerAlgorithm, inputPath);
        await File.WriteAllTextAsync(resultPath, report, Encoding.UTF8);

        Console.WriteLine();
        Console.WriteLine(Resources.HeaderBorder);
        Console.WriteLine(Resources.HeaderTitle);
        Console.WriteLine(Resources.HeaderBorder);
        Console.WriteLine(ExtractSummarySection(report));
        Console.WriteLine(string.Format(Resources.FullReportWritten, resultPath));

        long peakMemory = Process.GetCurrentProcess().PeakWorkingSet64;
        double peakMemoryMb = peakMemory / (1024.0 * 1024.0);
        double peakMemoryGb = peakMemory / (1024.0 * 1024.0 * 1024.0);
        Console.WriteLine(string.Format(Resources.PeakMemory, peakMemoryMb, peakMemoryGb));
    }

    // ── Statistics ────────────────────────────────────────────────────────────

    /// <summary>
    /// Returns the median elapsed time for successful runs of one algorithm.
    /// Uses the lower median for even counts (standard practice).
    /// Returns TimeSpan.Zero if there are no successful runs.
    /// </summary>
    private static TimeSpan ComputeMedian(IEnumerable<RunResult> results)
    {
        var sorted = results
            .Where(r => r.Success)
            .Select(r => r.Elapsed)
            .OrderBy(t => t)
            .ToList();

        if (sorted.Count == 0) return TimeSpan.Zero;

        var mid = sorted.Count / 2;
        // Even count → lower median (avoids averaging two TimeSpans, keeps it simple)
        return sorted.Count % 2 == 1 ? sorted[mid] : sorted[mid - 1];
    }

    // ── Report builder ────────────────────────────────────────────────────────

    /// <summary>
    /// Compiles all run results and generates a structured, human-readable text report.
    /// </summary>
    private static string BuildReport(
        IReadOnlyList<ISortAlgorithm> algorithms,
        List<RunResult>               allResults,
        int                           runsPerAlgorithm,
        string                        inputPath)
    {
        var sb = new StringBuilder();

        sb.AppendLine(Resources.ReportHeaderBorderTop);
        sb.AppendLine(Resources.ReportHeaderTitle);
        sb.AppendLine(Resources.ReportHeaderBorderBottom);
        sb.AppendLine();
        sb.AppendLine(string.Format(Resources.ReportDate, DateTime.Now));
        sb.AppendLine(string.Format(Resources.ReportInputFile, inputPath));
        try 
        { 
            double sizeMb = new FileInfo(inputPath).Length / (1024.0 * 1024.0);
            sb.AppendLine(string.Format(Resources.ReportFileSize, sizeMb)); 
        }
        catch 
        { 
            sb.AppendLine(Resources.ReportFileSizeUnavailable); 
        }
        sb.AppendLine(string.Format(Resources.ReportRunsPerAlgo, runsPerAlgorithm));
        sb.AppendLine(string.Format(Resources.ReportDotNetVersion, Environment.Version));
        sb.AppendLine();

        // ── Per-algorithm detail ──────────────────────────────────────────────
        sb.AppendLine(Resources.ReportSectionBorder);
        sb.AppendLine(Resources.ReportPerAlgoResultsTitle);
        sb.AppendLine(Resources.ReportSectionBorder);

        var medians = new Dictionary<string, TimeSpan>();

        foreach (var algo in algorithms)
        {
            var runs   = allResults.Where(r => r.AlgorithmName == algo.Name).ToList();
            var median = ComputeMedian(runs);
            var min    = runs.Where(r => r.Success).Select(r => r.Elapsed).DefaultIfEmpty().Min();
            var max    = runs.Where(r => r.Success).Select(r => r.Elapsed).DefaultIfEmpty().Max();
            var ok     = runs.Count(r => r.Success);

            medians[algo.Name] = median;

            sb.AppendLine();
            sb.AppendLine(string.Format(Resources.ReportAlgoName, algo.Name));
            sb.AppendLine(string.Format(Resources.ReportAlgoDesc, algo.Description));
            sb.AppendLine(string.Format(Resources.ReportAlgoSuccess, ok, runsPerAlgorithm));
            sb.AppendLine(string.Format(Resources.ReportAlgoMedian, FormatTs(median)));
            sb.AppendLine(string.Format(Resources.ReportAlgoMin, FormatTs(min)));
            sb.AppendLine(string.Format(Resources.ReportAlgoMax, FormatTs(max)));

            if (ok > 1)
            {
                var times = runs.Where(r => r.Success).Select(r => r.Elapsed.TotalMilliseconds).ToList();
                var avg    = times.Average();
                var stdDev = Math.Sqrt(times.Select(t => Math.Pow(t - avg, 2)).Average());
                sb.AppendLine(string.Format(Resources.ReportAlgoAvg, TimeSpan.FromMilliseconds(avg)));
                sb.AppendLine(string.Format(Resources.ReportAlgoStdDev, stdDev / 1000.0));
            }

            sb.AppendLine();
            sb.AppendLine(Resources.ReportRunByRunHeader);
            foreach (var r in runs)
            {
                var marker = r.Success ? FormatTs(r.Elapsed) : string.Format(Resources.ReportRunFailed, r.Error);
                sb.AppendLine(string.Format(Resources.ReportRunLine, r.RunNumber, marker));
            }
        }

        // ── Summary table ─────────────────────────────────────────────────────
        sb.AppendLine();
        sb.AppendLine(Resources.ReportSectionBorder);
        sb.AppendLine(Resources.ReportSummaryTitle);
        sb.AppendLine(Resources.ReportSectionBorder);
        sb.AppendLine();

        var ranked = medians
            .Where(kv => kv.Value > TimeSpan.Zero)
            .OrderBy(kv => kv.Value)
            .ToList();

        if (ranked.Count == 0)
        {
            sb.AppendLine(Resources.ReportNoSuccessfulRuns);
        }
        else
        {
            var fastest        = ranked[0];
            var nameColWidth   = ranked.Max(kv => kv.Key.Length) + 2;

            sb.AppendLine(string.Format(Resources.ReportHeaderCols, Resources.ColHeaderAlgorithm.PadRight(nameColWidth), Resources.ColHeaderMedian, Resources.ColHeaderVsFastest, Resources.ColHeaderVsPrevious));
            sb.AppendLine(string.Format(Resources.ReportHeaderCols, new string('-', nameColWidth), "----------", "----------", "-----------"));

            TimeSpan? prev = null;
            for (var i = 0; i < ranked.Count; i++)
            {
                var (name, median) = (ranked[i].Key, ranked[i].Value);
                var vsFastest = i == 0 ? Resources.ReportBestMarker : string.Format(Resources.ReportSlowerMarker, (median - fastest.Value).TotalSeconds, median.TotalMilliseconds / fastest.Value.TotalMilliseconds);
                var vsPrev    = prev == null ? Resources.ReportNoPrevMarker : string.Format(Resources.ReportVsPrevMarker, (median - prev.Value).TotalSeconds);
                var tag       = i == 0 ? Resources.ReportFastestIndicator : "";

                sb.AppendLine(string.Format(Resources.ReportRankLine, name.PadRight(nameColWidth), FormatTs(median), vsFastest, vsPrev, tag));
                prev = median;
            }

            sb.AppendLine();
            sb.AppendLine(string.Format(Resources.ReportFastestSummary, fastest.Key, FormatTs(fastest.Value)));

            // Speedup narrative
            if (ranked.Count > 1)
            {
                var slowest = ranked[^1];
                var speedup = slowest.Value.TotalMilliseconds / fastest.Value.TotalMilliseconds;
                sb.AppendLine(string.Format(Resources.ReportSpeedupSummary, speedup, slowest.Key));
            }
        }

        sb.AppendLine();
        sb.AppendLine(Resources.ReportSectionBorder);
        sb.AppendLine(Resources.ReportEndOfReport);
        sb.AppendLine(Resources.ReportSectionBorder);

        return sb.ToString();
    }

    // ── Console helpers ───────────────────────────────────────────────────────

    /// <summary>
    /// Prints the benchmark execution header to the console.
    /// </summary>
    private static void PrintHeader(IReadOnlyList<ISortAlgorithm> algorithms, int runs, string input)
    {
        Console.WriteLine(Resources.HeaderBorder);
        Console.WriteLine(Resources.HeaderTitleStart);
        Console.WriteLine(Resources.HeaderBorder);
        Console.WriteLine(string.Format(Resources.HeaderInput, input));
        Console.WriteLine(string.Format(Resources.HeaderAlgorithmsCount, algorithms.Count));
        Console.WriteLine(string.Format(Resources.HeaderRunsPerAlgo, runs));
        Console.WriteLine(string.Format(Resources.HeaderTotalRuns, algorithms.Count * runs));
        Console.WriteLine(Resources.HeaderBorder);
    }

    /// <summary>
    /// Prints a visual progress bar indicating benchmark execution completion percentage.
    /// </summary>
    private static void PrintProgress(int done, int total)
    {
        int width   = 40;
        int filled  = (int)Math.Round((double)done / total * width);
        string bar  = new string('█', filled) + new string('░', width - filled);
        Console.Write(string.Format(Resources.ProgressLine, bar, done, total));
        if (done == total) Console.WriteLine();
    }

    /// <summary>
    /// Extracts the final summary section from the report text to print to the console.
    /// </summary>
    private static string ExtractSummarySection(string report)
    {
        var idx = report.IndexOf("SUMMARY", StringComparison.Ordinal);
        return idx < 0 ? report : report[idx..];
    }

    /// <summary>
    /// Formats a <see cref="TimeSpan"/> to a fixed-width string template.
    /// </summary>
    private static string FormatTs(TimeSpan ts) =>
        ts == TimeSpan.Zero ? "  --:--   " : ts.ToString(@"mm\:ss\.fff");

    /// <summary>
    /// Replaces non-alphanumeric characters with underscores to ensure file-system-safe filenames.
    /// </summary>
    private static string SanitiseName(string name) =>
        string.Concat(name.Select(c => char.IsLetterOrDigit(c) ? c : '_'));
}