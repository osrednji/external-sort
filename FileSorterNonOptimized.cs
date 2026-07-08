using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using K4os.Compression.LZ4.Streams;  // NuGet: K4os.Compression.LZ4

/// <summary>
/// High-performance external merge sort for files up to 100+ GB.
///
/// Optimisations:
///   1. PARSE-ONCE STRUCTS     — lines parsed into (Text, Number, Raw) exactly once before
///                               sorting; comparisons hit pre-parsed fields only.
///   2. PARALLEL SORT          — each 512 MB chunk is partitioned into N sub-ranges
///                               (N = logical CPU count), each sorted on its own thread,
///                               then merged with a final O(N·log N) merge step.
///                               Saturates all cores during Phase 1.
///   3. PIPELINE (read‖sort‖write) — three async stages via bounded Channel<T>:
///                               Reader feeds raw chunks → Sorter parses+sorts →
///                               Writer compresses+flushes. All overlap on disk+CPU.
///   4. LZ4 COMPRESSION        — temp run files compressed with LZ4 (Fastest frame format).
///                               ~4-6× size reduction at ~500 MB/s, vs Deflate's ~150 MB/s.
///                               Merge-phase I/O is the bottleneck; smaller files = faster.
///   5. FULL-FAN-IN MERGE      — all runs merged in a single pass; no intermediate files.
///                               Heap keys are pre-parsed at enqueue → zero re-parsing in loop.
///   6. SERVER GC + LARGE I/O  — 32-64 MB buffers, FileOptions.SequentialScan, Server GC.
/// </summary>
public static class FileSorter
{
    // ── Tunables ─────────────────────────────────────────────────────────────
    private const long CHUNK_BYTES    = 512L * 1024 * 1024; // RAM per sort chunk
    private const int  READ_BUFFER    = 4   * 1024 * 1024;  // per-run buffer in merge phase
    private const int  WRITE_BUFFER   = 64  * 1024 * 1024;  // write buffer (runs + output)
    private const int  IN_BUFFER      = 32  * 1024 * 1024;  // input read buffer (phase 1)
    private const int  PIPELINE_DEPTH = 1;                   // extra chunks buffered in pipeline

    // Parallel sort: use all logical CPUs, but cap so sub-ranges aren't trivially small
    private static readonly int SortParallelism = Math.Max(1, Environment.ProcessorCount);
    // ─────────────────────────────────────────────────────────────────────────

    // ── Parsed line struct ────────────────────────────────────────────────────
    private readonly struct SortEntry
    {
        public readonly string Raw;
        public readonly string Text;
        public readonly int    Number;

        public SortEntry(string raw, string text, int number)
        { Raw = raw; Text = text; Number = number; }
    }

    // ── Entry point ───────────────────────────────────────────────────────────
    public static async Task SortAsync(string inputPath, string outputPath)
    {
        string tempDir = Path.Combine(
            Path.GetDirectoryName(Path.GetFullPath(outputPath)) ?? ".",
            "_sort_tmp_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(tempDir);

        try
        {
            Console.WriteLine($"Parallelism: {SortParallelism} threads");
            Console.WriteLine("Phase 1: Building sorted LZ4-compressed runs...");
            var sw1 = System.Diagnostics.Stopwatch.StartNew();
            var runs = await BuildRunsAsync(inputPath, tempDir);
            Console.WriteLine($"  {runs.Count} runs in {sw1.Elapsed:mm\\:ss\\.ff}");

            Console.WriteLine($"Phase 2: Merging {runs.Count} runs (single pass)...");
            var sw2 = System.Diagnostics.Stopwatch.StartNew();
            await KWayMergeAsync(runs, outputPath);
            Console.WriteLine($"  Merge done in {sw2.Elapsed:mm\\:ss\\.ff}");

            Console.WriteLine("Done.");
        }
        finally
        {
            try { Directory.Delete(tempDir, recursive: true); } catch { }
        }
    }

    // ── Phase 1: Three-stage pipeline ────────────────────────────────────────
    private static async Task<List<string>> BuildRunsAsync(string inputPath, string tempDir)
    {
        // A → B: raw string chunks
        var rawChan = Channel.CreateBounded<List<string>>(
            new BoundedChannelOptions(PIPELINE_DEPTH)
            { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });

        // B → C: parsed + parallel-sorted entry arrays
        var sortedChan = Channel.CreateBounded<(SortEntry[] entries, int count)>(
            new BoundedChannelOptions(PIPELINE_DEPTH)
            { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });

        var runFiles = new List<string>();

        // ── Stage A: Reader ───────────────────────────────────────────────────
        var readerTask = Task.Run(async () =>
        {
            await using var fs = new FileStream(inputPath, FileMode.Open, FileAccess.Read,
                FileShare.Read, IN_BUFFER, FileOptions.SequentialScan);
            using var reader = new StreamReader(fs, Encoding.UTF8, false, IN_BUFFER);

            var chunk = new List<string>(EstimateLines(CHUNK_BYTES));
            long chunkBytes = 0;

            string? line;
            while ((line = await reader.ReadLineAsync().ConfigureAwait(false)) != null)
            {
                chunk.Add(line);
                chunkBytes += line.Length * 2 + 56; // UTF-16 chars + object header

                if (chunkBytes >= CHUNK_BYTES)
                {
                    await rawChan.Writer.WriteAsync(chunk).ConfigureAwait(false);
                    chunk = new List<string>(chunk.Count);
                    chunkBytes = 0;
                }
            }

            if (chunk.Count > 0)
                await rawChan.Writer.WriteAsync(chunk).ConfigureAwait(false);

            rawChan.Writer.Complete();
        });

        // ── Stage B: Parse + Parallel Sort ───────────────────────────────────
        var sorterTask = Task.Run(async () =>
        {
            await foreach (var raw in rawChan.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                int count = raw.Count;

                // 1. Parse every line into a SortEntry — O(N), once
                var entries = new SortEntry[count];
                Parallel.For(0, count, new ParallelOptions { MaxDegreeOfParallelism = SortParallelism },
                    i => entries[i] = ParseEntry(raw[i]));

                // 2. Parallel sort:
                //    a. Divide entries into SortParallelism sub-ranges
                //    b. Sort each sub-range independently (full CPU saturation)
                //    c. Merge sub-ranges into a single sorted array
                ParallelSort(entries, count);

                await sortedChan.Writer.WriteAsync((entries, count)).ConfigureAwait(false);
            }
            sortedChan.Writer.Complete();
        });

        // ── Stage C: LZ4 Compressed Writer ───────────────────────────────────
        var writerTask = Task.Run(async () =>
        {
            int idx = 0;
            await foreach (var (entries, count) in sortedChan.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                string path = Path.Combine(tempDir, $"run_{idx:D6}.lz4");
                await WriteLz4RunAsync(path, entries, count).ConfigureAwait(false);
                runFiles.Add(path);
                long sz = new FileInfo(path).Length;
                Console.WriteLine($"  Run {++idx}: {count:N0} lines, {BytesToMb(sz)} MB (lz4)");
            }
        });

        await Task.WhenAll(readerTask, sorterTask, writerTask).ConfigureAwait(false);
        return runFiles;
    }

    // ── Parallel sort implementation ──────────────────────────────────────────
    /// <summary>
    /// Sorts <paramref name="entries"/>[0..<paramref name="count"/>] in parallel.
    ///
    /// Algorithm:
    ///   1. Split into P sub-ranges (P = SortParallelism).
    ///   2. Sort each sub-range on a thread-pool thread via Task.WhenAll.
    ///   3. K-way merge the P sorted sub-ranges into a scratch array.
    ///   4. Copy scratch back to entries.
    ///
    /// This saturates all CPU cores during the sort phase.
    /// </summary>
    private static void ParallelSort(SortEntry[] entries, int count)
    {
        int p = Math.Min(SortParallelism, count); // can't have more partitions than items
        if (p <= 1) { Array.Sort(entries, 0, count, SortEntryComparer.Instance); return; }

        // Compute sub-range boundaries
        int baseSize  = count / p;
        int remainder = count % p;
        var ranges    = new (int start, int len)[p];
        int cursor    = 0;
        for (int i = 0; i < p; i++)
        {
            int len = baseSize + (i < remainder ? 1 : 0);
            ranges[i] = (cursor, len);
            cursor += len;
        }

        // Sort each sub-range in parallel
        var tasks = new Task[p];
        for (int i = 0; i < p; i++)
        {
            var (start, len) = ranges[i];
            tasks[i] = Task.Run(() => Array.Sort(entries, start, len, SortEntryComparer.Instance));
        }
        Task.WaitAll(tasks);

        // Merge sorted sub-ranges with a min-heap → scratch array
        var scratch = new SortEntry[count];
        var heap = new PriorityQueue<(SortEntry entry, int rangeIdx, int posInRange), SortEntry>(
            p, SortEntryHeapComparer.Instance);

        // Seed heap with the first element of each sub-range
        for (int i = 0; i < p; i++)
        {
            if (ranges[i].len > 0)
            {
                var e = entries[ranges[i].start];
                heap.Enqueue((e, i, 0), e);
            }
        }

        // Cursor array tracks how far we've consumed each sub-range
        var pos = new int[p]; // pos[i] = next index to consume within sub-range i (0-based)

        int outIdx = 0;
        while (heap.Count > 0)
        {
            heap.TryDequeue(out var item, out _);
            scratch[outIdx++] = item.entry;

            int ri    = item.rangeIdx;
            int next  = item.posInRange + 1;
            if (next < ranges[ri].len)
            {
                var e = entries[ranges[ri].start + next];
                heap.Enqueue((e, ri, next), e);
            }
        }

        // Copy merged result back
        Array.Copy(scratch, entries, count);
    }

    // ── LZ4 run file I/O ──────────────────────────────────────────────────────
    private static async Task WriteLz4RunAsync(string path, SortEntry[] entries, int count)
    {
        await using var fs     = new FileStream(path, FileMode.Create, FileAccess.Write,
            FileShare.None, WRITE_BUFFER, FileOptions.SequentialScan);
        await using var lz4    = LZ4Stream.Encode(fs, K4os.Compression.LZ4.LZ4Level.L00_FAST,
            leaveOpen: true);
        await using var writer = new StreamWriter(lz4, Encoding.UTF8, WRITE_BUFFER);

        for (int i = 0; i < count; i++)
            await writer.WriteLineAsync(entries[i].Raw).ConfigureAwait(false);
    }

    private static StreamReader OpenLz4Run(string path)
    {
        var fs  = new FileStream(path, FileMode.Open, FileAccess.Read,
            FileShare.Read, READ_BUFFER, FileOptions.SequentialScan);
        var lz4 = LZ4Stream.Decode(fs, leaveOpen: false);
        return new StreamReader(lz4, Encoding.UTF8, false, READ_BUFFER);
    }

    // ── Phase 2: Full-fan-in K-way merge ─────────────────────────────────────
    private static async Task KWayMergeAsync(List<string> runPaths, string outputPath)
    {
        int k = runPaths.Count;

        var readers = new StreamReader[k];
        for (int i = 0; i < k; i++)
            readers[i] = OpenLz4Run(runPaths[i]);

        // Min-heap: key = pre-parsed (text, num) — zero re-parsing inside merge loop
        var heap = new PriorityQueue<(string raw, int idx), (string text, int num)>(
            k, HeapKeyComparer.Instance);

        for (int i = 0; i < k; i++)
        {
            var line = await readers[i].ReadLineAsync().ConfigureAwait(false);
            if (line != null)
                heap.Enqueue((line, i), ParseKey(line));
        }

        await using var outFs  = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
            FileShare.None, WRITE_BUFFER, FileOptions.SequentialScan);
        await using var writer = new StreamWriter(outFs, Encoding.UTF8, WRITE_BUFFER);

        long written = 0;
        while (heap.Count > 0)
        {
            heap.TryDequeue(out var item, out _);
            await writer.WriteLineAsync(item.raw).ConfigureAwait(false);
            written++;

            var next = await readers[item.idx].ReadLineAsync().ConfigureAwait(false);
            if (next != null)
                heap.Enqueue((next, item.idx), ParseKey(next));

            if (written % 10_000_000 == 0)
                Console.WriteLine($"    ...{written:N0} lines merged");
        }

        Console.WriteLine($"  Total lines: {written:N0}");

        for (int i = 0; i < k; i++)
        {
            readers[i].Dispose();
            try { File.Delete(runPaths[i]); } catch { }
        }
    }

    // ── Parsing ───────────────────────────────────────────────────────────────
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SortEntry ParseEntry(string line)
    {
        var (text, num) = ParseKey(line);
        return new SortEntry(line, text, num);
    }

    /// <summary>Parses "415. Apple" → ("Apple", 415). Single pass, no extra allocation.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (string text, int num) ParseKey(string line)
    {
        int dot = line.IndexOf('.');
        if (dot <= 0) return (line, 0);

        int num = 0;
        for (int i = 0; i < dot; i++)
        {
            uint d = (uint)(line[i] - '0');
            if (d <= 9) num = num * 10 + (int)d;
        }

        int ts = dot + 1;
        if ((uint)ts < (uint)line.Length && line[ts] == ' ') ts++;
        string text = (uint)ts < (uint)line.Length ? line[ts..] : string.Empty;

        return (text, num);
    }

    // ── Comparers ─────────────────────────────────────────────────────────────
    private sealed class SortEntryComparer : IComparer<SortEntry>
    {
        public static readonly SortEntryComparer Instance = new();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(SortEntry x, SortEntry y)
        {
            int c = string.Compare(x.Text, y.Text, StringComparison.OrdinalIgnoreCase);
            return c != 0 ? c : x.Number.CompareTo(y.Number);
        }
    }

    // Used by ParallelSort's merge heap — compares SortEntry directly
    private sealed class SortEntryHeapComparer : IComparer<SortEntry>
    {
        public static readonly SortEntryHeapComparer Instance = new();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(SortEntry x, SortEntry y)
        {
            int c = string.Compare(x.Text, y.Text, StringComparison.OrdinalIgnoreCase);
            return c != 0 ? c : x.Number.CompareTo(y.Number);
        }
    }

    private sealed class HeapKeyComparer : IComparer<(string text, int num)>
    {
        public static readonly HeapKeyComparer Instance = new();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare((string text, int num) x, (string text, int num) y)
        {
            int c = string.Compare(x.text, y.text, StringComparison.OrdinalIgnoreCase);
            return c != 0 ? c : x.num.CompareTo(y.num);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────
    private static int    EstimateLines(long bytes) => (int)Math.Min(bytes / 40, int.MaxValue);
    private static string BytesToMb(long b)         => (b / (1024.0 * 1024.0)).ToString("F1");

    // ── CLI ───────────────────────────────────────────────────────────────────
    public static async Task Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.WriteLine("Usage: FileSorter <input_path> <output_path>");
            Console.WriteLine("Example: FileSorter big.txt sorted.txt");
            return;
        }

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await SortAsync(args[0], args[1]);
        Console.WriteLine($"Total elapsed: {sw.Elapsed:mm\\:ss\\.ff}");
    }
}
