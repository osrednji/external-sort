using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using K4os.Compression.LZ4.Streams; // NuGet: K4os.Compression.LZ4

// ═══════════════════════════════════════════════════════════════════════════════
//  Shared infrastructure used by all three algorithm implementations
// ═══════════════════════════════════════════════════════════════════════════════

internal readonly struct SortEntry
{
    public readonly string Raw;
    public readonly string Text;
    public readonly int    Number;
    public SortEntry(string raw, string text, int number)
    { Raw = raw; Text = text; Number = number; }
}

internal sealed class SortEntryComparer : IComparer<SortEntry>
{
    public static readonly SortEntryComparer Instance = new();
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(SortEntry x, SortEntry y)
    {
        int c = string.Compare(x.Text, y.Text, StringComparison.OrdinalIgnoreCase);
        return c != 0 ? c : x.Number.CompareTo(y.Number);
    }
}

internal sealed class HeapKeyComparer : IComparer<(string text, int num)>
{
    public static readonly HeapKeyComparer Instance = new();
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare((string text, int num) x, (string text, int num) y)
    {
        int c = string.Compare(x.text, y.text, StringComparison.OrdinalIgnoreCase);
        return c != 0 ? c : x.num.CompareTo(y.num);
    }
}

internal static class LineParser
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (string text, int num) ParseKey(string line)
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
        return ((uint)ts < (uint)line.Length ? line[ts..] : string.Empty, num);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SortEntry ParseEntry(string line)
    {
        var (text, num) = ParseKey(line);
        return new SortEntry(line, text, num);
    }
}

internal static class AlgoHelper
{
    public static string MakeTempDir(string outputPath) =>
        Directory.CreateDirectory(Path.Combine(
            Path.GetDirectoryName(Path.GetFullPath(outputPath)) ?? ".",
            "_tmp_" + Guid.NewGuid().ToString("N")[..8])).FullName;

    public static void CleanUp(string dir)
    { try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { } }

    public static int EstimateLines(long bytes) => (int)Math.Min(bytes / 40, int.MaxValue);
}


// ═══════════════════════════════════════════════════════════════════════════════
//  V1 — BASELINE
// ═══════════════════════════════════════════════════════════════════════════════
public sealed class BaselineSortAlgorithm : ISortAlgorithm
{
    public string Name        => "V1 Baseline";
    public string Description => "Sequential sort, no compression, key re-parsed on every comparison, fan-in=8 (multi-pass merge possible).";

    private const long CHUNK     = 512L * 1024 * 1024;
    private const int  RBUF      = 32   * 1024 * 1024;
    private const int  WBUF      = 64   * 1024 * 1024;
    private const int  FAN       = 8;

    public async Task SortAsync(string inputPath, string outputPath)
    {
        string tmp = AlgoHelper.MakeTempDir(outputPath);
        try
        {
            var runs = await BuildRunsAsync(inputPath, tmp);
            await MergeAsync(runs, outputPath, tmp);
        }
        finally { AlgoHelper.CleanUp(tmp); }
    }

    private async Task<List<string>> BuildRunsAsync(string input, string tmp)
    {
        var files = new List<string>(); int idx = 0;
        var chan   = Channel.CreateBounded<List<string>>(new BoundedChannelOptions(2)
            { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });

        var prod = Task.Run(async () =>
        {
            await using var fs = new FileStream(input, FileMode.Open, FileAccess.Read, FileShare.Read, RBUF, FileOptions.SequentialScan);
            using var rdr = new StreamReader(fs, Encoding.UTF8, false, RBUF);
            var chunk = new List<string>(AlgoHelper.EstimateLines(CHUNK)); long cb = 0;
            string? line;
            while ((line = await rdr.ReadLineAsync()) != null)
            {
                chunk.Add(line); cb += line.Length * 2 + 64;
                if (cb >= CHUNK) { await chan.Writer.WriteAsync(chunk); chunk = new(chunk.Count); cb = 0; }
            }
            if (chunk.Count > 0) await chan.Writer.WriteAsync(chunk);
            chan.Writer.Complete();
        });

        await foreach (var chunk in chan.Reader.ReadAllAsync())
        {
            chunk.Sort(new LegacyComparer());
            string p = Path.Combine(tmp, $"r{idx++:D6}.tmp");
            await WritePlainAsync(p, chunk);
            files.Add(p);
        }
        await prod; return files;
    }

    private async Task MergeAsync(List<string> runs, string output, string tmp)
    {
        while (runs.Count > FAN)
        {
            var next = new List<string>();
            for (int i = 0; i < runs.Count; i += FAN)
            {
                var batch  = runs.GetRange(i, Math.Min(FAN, runs.Count - i));
                string m   = Path.Combine(tmp, $"m{Guid.NewGuid():N}.tmp");
                await KMergePlainAsync(batch, m);
                foreach (var r in batch) File.Delete(r);
                next.Add(m);
            }
            runs = next;
        }
        await KMergePlainAsync(runs, output);
        foreach (var r in runs) try { File.Delete(r); } catch { }
    }

    private async Task KMergePlainAsync(List<string> paths, string output)
    {
        int k = paths.Count; var rdrs = new StreamReader[k];
        var heap = new PriorityQueue<(string l, int i), (string t, int n)>(k, HeapKeyComparer.Instance);
        for (int i = 0; i < k; i++)
        {
            var fs = new FileStream(paths[i], FileMode.Open, FileAccess.Read, FileShare.Read, RBUF, FileOptions.SequentialScan);
            rdrs[i] = new StreamReader(fs, Encoding.UTF8, false, RBUF);
            var l = await rdrs[i].ReadLineAsync();
            if (l != null) heap.Enqueue((l, i), LineParser.ParseKey(l));
        }
        await using var ofs = new FileStream(output, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        await using var wtr = new StreamWriter(ofs, Encoding.UTF8, WBUF);
        while (heap.Count > 0)
        {
            heap.TryDequeue(out var item, out _);
            await wtr.WriteLineAsync(item.l);
            var nx = await rdrs[item.i].ReadLineAsync();
            if (nx != null) heap.Enqueue((nx, item.i), LineParser.ParseKey(nx));
        }
        foreach (var r in rdrs) r.Dispose();
    }

    private async Task WritePlainAsync(string path, List<string> lines)
    {
        await using var fs  = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        await using var wtr = new StreamWriter(fs, Encoding.UTF8, WBUF);
        foreach (var l in lines) await wtr.WriteLineAsync(l);
    }

    // Intentionally re-parses on every comparison — v1 behaviour
    private sealed class LegacyComparer : IComparer<string>
    {
        public int Compare(string? x, string? y)
        {
            if (x == null && y == null) return 0;
            if (x == null) return -1; if (y == null) return 1;
            var (xt, xn) = LineParser.ParseKey(x);
            var (yt, yn) = LineParser.ParseKey(y);
            int c = string.Compare(xt, yt, StringComparison.OrdinalIgnoreCase);
            return c != 0 ? c : xn.CompareTo(yn);
        }
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  V2 — PIPELINE + PARSE-ONCE + DEFLATE + FULL FAN-IN
// ═══════════════════════════════════════════════════════════════════════════════
public sealed class PipelineDeflateSortAlgorithm : ISortAlgorithm
{
    public string Name        => "V2 Pipeline+Deflate";
    public string Description => "Parse-once structs, 3-stage async pipeline (read/sort/write overlap), Deflate-compressed temp files, full-fan-in single-pass merge.";

    private const long CHUNK = 512L * 1024 * 1024;
    private const int  RBUF  = 4   * 1024 * 1024;
    private const int  WBUF  = 64  * 1024 * 1024;
    private const int  IBUF  = 32  * 1024 * 1024;
    private const int  PDEPTH = 1;

    public async Task SortAsync(string inputPath, string outputPath)
    {
        string tmp = AlgoHelper.MakeTempDir(outputPath);
        try   { var runs = await BuildAsync(inputPath, tmp); await KMergeAsync(runs, outputPath); }
        finally { AlgoHelper.CleanUp(tmp); }
    }

    private async Task<List<string>> BuildAsync(string input, string tmp)
    {
        var rawCh  = Channel.CreateBounded<List<string>>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var srtCh  = Channel.CreateBounded<(SortEntry[] e, int n)>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var files  = new List<string>();

        var rdr = Task.Run(async () =>
        {
            await using var fs = new FileStream(input, FileMode.Open, FileAccess.Read, FileShare.Read, IBUF, FileOptions.SequentialScan);
            using var r = new StreamReader(fs, Encoding.UTF8, false, IBUF);
            var chunk = new List<string>(AlgoHelper.EstimateLines(CHUNK)); long cb = 0; string? line;
            while ((line = await r.ReadLineAsync().ConfigureAwait(false)) != null)
            {
                chunk.Add(line); cb += line.Length * 2 + 56;
                if (cb >= CHUNK) { await rawCh.Writer.WriteAsync(chunk).ConfigureAwait(false); chunk = new(chunk.Count); cb = 0; }
            }
            if (chunk.Count > 0) await rawCh.Writer.WriteAsync(chunk).ConfigureAwait(false);
            rawCh.Writer.Complete();
        });

        var srt = Task.Run(async () =>
        {
            await foreach (var raw in rawCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                var e = new SortEntry[raw.Count];
                for (int i = 0; i < raw.Count; i++) e[i] = LineParser.ParseEntry(raw[i]);
                Array.Sort(e, 0, raw.Count, SortEntryComparer.Instance);
                await srtCh.Writer.WriteAsync((e, raw.Count)).ConfigureAwait(false);
            }
            srtCh.Writer.Complete();
        });

        var wrt = Task.Run(async () =>
        {
            int idx = 0;
            await foreach (var (e, n) in srtCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                string p = Path.Combine(tmp, $"r{idx++:D6}.bin");
                await WriteDeflateAsync(p, e, n).ConfigureAwait(false);
                files.Add(p);
            }
        });

        await Task.WhenAll(rdr, srt, wrt).ConfigureAwait(false);
        return files;
    }

    private async Task WriteDeflateAsync(string path, SortEntry[] e, int n)
    {
        await using var fs  = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        await using var dfl = new DeflateStream(fs, CompressionLevel.Fastest, leaveOpen: true);
        await using var wtr = new StreamWriter(dfl, Encoding.UTF8, WBUF);
        for (int i = 0; i < n; i++) await wtr.WriteLineAsync(e[i].Raw).ConfigureAwait(false);
    }

    private StreamReader OpenDeflate(string path)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, RBUF, FileOptions.SequentialScan);
        return new StreamReader(new DeflateStream(fs, CompressionMode.Decompress), Encoding.UTF8, false, RBUF);
    }

    private async Task KMergeAsync(List<string> paths, string output)
    {
        int k = paths.Count; var rdrs = new StreamReader[k];
        for (int i = 0; i < k; i++) rdrs[i] = OpenDeflate(paths[i]);
        var heap = new PriorityQueue<(string l, int i), (string t, int n)>(k, HeapKeyComparer.Instance);
        for (int i = 0; i < k; i++) { var l = await rdrs[i].ReadLineAsync().ConfigureAwait(false); if (l != null) heap.Enqueue((l, i), LineParser.ParseKey(l)); }
        await using var ofs = new FileStream(output, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        await using var wtr = new StreamWriter(ofs, Encoding.UTF8, WBUF);
        while (heap.Count > 0)
        {
            heap.TryDequeue(out var item, out _);
            await wtr.WriteLineAsync(item.l).ConfigureAwait(false);
            var nx = await rdrs[item.i].ReadLineAsync().ConfigureAwait(false);
            if (nx != null) heap.Enqueue((nx, item.i), LineParser.ParseKey(nx));
        }
        for (int i = 0; i < k; i++) { rdrs[i].Dispose(); try { File.Delete(paths[i]); } catch { } }
    }
}


// ═══════════════════════════════════════════════════════════════════════════════
//  V3 — LZ4 + PARALLEL SORT + FULL STACK
// ═══════════════════════════════════════════════════════════════════════════════
public sealed class Lz4ParallelSortAlgorithm : ISortAlgorithm
{
    public string Name        => "V3 LZ4+ParallelSort";
    public string Description => "All V2 optimisations + parallel parse/sort across all CPU cores + LZ4 compression (~500 MB/s vs Deflate ~150 MB/s).";

    private const long CHUNK  = 512L * 1024 * 1024;
    private const int  RBUF   = 4   * 1024 * 1024;
    private const int  WBUF   = 64  * 1024 * 1024;
    private const int  IBUF   = 32  * 1024 * 1024;
    private const int  PDEPTH = 1;
    private static readonly int P = Math.Max(1, Environment.ProcessorCount);

    public async Task SortAsync(string inputPath, string outputPath)
    {
        string tmp = AlgoHelper.MakeTempDir(outputPath);
        try   { var runs = await BuildAsync(inputPath, tmp); await KMergeAsync(runs, outputPath); }
        finally { AlgoHelper.CleanUp(tmp); }
    }

    private async Task<List<string>> BuildAsync(string input, string tmp)
    {
        var rawCh = Channel.CreateBounded<List<string>>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var srtCh = Channel.CreateBounded<(SortEntry[] e, int n)>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var files = new List<string>();

        var rdr = Task.Run(async () =>
        {
            await using var fs = new FileStream(input, FileMode.Open, FileAccess.Read, FileShare.Read, IBUF, FileOptions.SequentialScan);
            using var r = new StreamReader(fs, Encoding.UTF8, false, IBUF);
            var chunk = new List<string>(AlgoHelper.EstimateLines(CHUNK)); long cb = 0; string? line;
            while ((line = await r.ReadLineAsync().ConfigureAwait(false)) != null)
            {
                chunk.Add(line); cb += line.Length * 2 + 56;
                if (cb >= CHUNK) { await rawCh.Writer.WriteAsync(chunk).ConfigureAwait(false); chunk = new(chunk.Count); cb = 0; }
            }
            if (chunk.Count > 0) await rawCh.Writer.WriteAsync(chunk).ConfigureAwait(false);
            rawCh.Writer.Complete();
        });

        var srt = Task.Run(async () =>
        {
            await foreach (var raw in rawCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                int count = raw.Count; var e = new SortEntry[count];
                Parallel.For(0, count, new ParallelOptions { MaxDegreeOfParallelism = P }, i => e[i] = LineParser.ParseEntry(raw[i]));
                ParallelSort(e, count);
                await srtCh.Writer.WriteAsync((e, count)).ConfigureAwait(false);
            }
            srtCh.Writer.Complete();
        });

        var wrt = Task.Run(async () =>
        {
            int idx = 0;
            await foreach (var (e, n) in srtCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                string p = Path.Combine(tmp, $"r{idx++:D6}.lz4");
                await WriteLz4Async(p, e, n).ConfigureAwait(false);
                files.Add(p);
            }
        });

        await Task.WhenAll(rdr, srt, wrt).ConfigureAwait(false);
        return files;
    }

    private static void ParallelSort(SortEntry[] entries, int count)
    {
        int p = Math.Min(P, count);
        if (p <= 1) { Array.Sort(entries, 0, count, SortEntryComparer.Instance); return; }

        int baseSize = count / p, rem = count % p;
        var ranges   = new (int s, int l)[p];
        int cur      = 0;
        for (int i = 0; i < p; i++) { int l = baseSize + (i < rem ? 1 : 0); ranges[i] = (cur, l); cur += l; }

        var tasks = new Task[p];
        for (int i = 0; i < p; i++) { var (s, l) = ranges[i]; tasks[i] = Task.Run(() => Array.Sort(entries, s, l, SortEntryComparer.Instance)); }
        Task.WaitAll(tasks);

        var scratch = new SortEntry[count];
        var heap    = new PriorityQueue<(SortEntry e, int ri, int pos), SortEntry>(p, ParallelMergeComparer.Instance);
        for (int i = 0; i < p; i++) if (ranges[i].l > 0) { var e = entries[ranges[i].s]; heap.Enqueue((e, i, 0), e); }

        int o = 0;
        while (heap.Count > 0)
        {
            heap.TryDequeue(out var item, out _);
            scratch[o++] = item.e;
            int nx = item.pos + 1;
            if (nx < ranges[item.ri].l) { var e = entries[ranges[item.ri].s + nx]; heap.Enqueue((e, item.ri, nx), e); }
        }
        Array.Copy(scratch, entries, count);
    }

    private static async Task WriteLz4Async(string path, SortEntry[] e, int n)
    {
        await using var fs  = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        await using var lz4 = LZ4Stream.Encode(fs, K4os.Compression.LZ4.LZ4Level.L00_FAST, leaveOpen: true);
        await using var wtr = new StreamWriter(lz4, Encoding.UTF8, WBUF);
        for (int i = 0; i < n; i++) await wtr.WriteLineAsync(e[i].Raw).ConfigureAwait(false);
    }

    private static StreamReader OpenLz4(string path)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, RBUF, FileOptions.SequentialScan);
        return new StreamReader(LZ4Stream.Decode(fs, leaveOpen: false), Encoding.UTF8, false, RBUF);
    }

    private async Task KMergeAsync(List<string> paths, string output)
    {
        int k = paths.Count; var rdrs = new StreamReader[k];
        for (int i = 0; i < k; i++) rdrs[i] = OpenLz4(paths[i]);
        var heap = new PriorityQueue<(string l, int i), (string t, int n)>(k, HeapKeyComparer.Instance);
        for (int i = 0; i < k; i++) { var l = await rdrs[i].ReadLineAsync().ConfigureAwait(false); if (l != null) heap.Enqueue((l, i), LineParser.ParseKey(l)); }
        await using var ofs = new FileStream(output, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        await using var wtr = new StreamWriter(ofs, Encoding.UTF8, WBUF);
        while (heap.Count > 0)
        {
            heap.TryDequeue(out var item, out _);
            await wtr.WriteLineAsync(item.l).ConfigureAwait(false);
            var nx = await rdrs[item.i].ReadLineAsync().ConfigureAwait(false);
            if (nx != null) heap.Enqueue((nx, item.i), LineParser.ParseKey(nx));
        }
        for (int i = 0; i < k; i++) { rdrs[i].Dispose(); try { File.Delete(paths[i]); } catch { } }
    }

    private sealed class ParallelMergeComparer : IComparer<SortEntry>
    {
        public static readonly ParallelMergeComparer Instance = new();
        public int Compare(SortEntry x, SortEntry y)
        {
            int c = string.Compare(x.Text, y.Text, StringComparison.OrdinalIgnoreCase);
            return c != 0 ? c : x.Number.CompareTo(y.Number);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  OPTIMIZED INFRASTRUCTURE (Zero-Allocation Key Representation)
// ═══════════════════════════════════════════════════════════════════════════════

internal readonly struct OptSortEntry
{
    public readonly string Raw;
    public readonly int KeyStartIndex;
    public readonly int Number;

    public OptSortEntry(string raw, int keyStartIndex, int number)
    {
        Raw = raw;
        KeyStartIndex = keyStartIndex;
        Number = number;
    }
}

internal sealed class OptSortEntryComparer : IComparer<OptSortEntry>
{
    public static readonly OptSortEntryComparer Instance = new();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(OptSortEntry x, OptSortEntry y)
    {
        int c = x.Raw.AsSpan(x.KeyStartIndex).CompareTo(y.Raw.AsSpan(y.KeyStartIndex), StringComparison.OrdinalIgnoreCase);
        return c != 0 ? c : x.Number.CompareTo(y.Number);
    }
}

internal readonly struct OptMergeEntry
{
    public readonly string Line;
    public readonly int KeyStartIndex;
    public readonly int Number;
    public readonly int ReaderIndex;

    public OptMergeEntry(string line, int keyStartIndex, int number, int readerIndex)
    {
        Line = line;
        KeyStartIndex = keyStartIndex;
        Number = number;
        ReaderIndex = readerIndex;
    }
}

internal sealed class OptMergeEntryComparer : IComparer<OptMergeEntry>
{
    public static readonly OptMergeEntryComparer Instance = new();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(OptMergeEntry x, OptMergeEntry y)
    {
        int c = x.Line.AsSpan(x.KeyStartIndex).CompareTo(y.Line.AsSpan(y.KeyStartIndex), StringComparison.OrdinalIgnoreCase);
        return c != 0 ? c : x.Number.CompareTo(y.Number);
    }
}

internal static class OptLineParser
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (int keyStart, int num) ParseKeyOffset(string line)
    {
        int dot = line.IndexOf('.');
        if (dot <= 0) return (0, 0);
        int num = 0;
        for (int i = 0; i < dot; i++)
        {
            uint d = (uint)(line[i] - '0');
            if (d <= 9) num = num * 10 + (int)d;
        }
        int ts = dot + 1;
        if ((uint)ts < (uint)line.Length && line[ts] == ' ') ts++;
        return (ts, num);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static OptSortEntry ParseEntry(string line)
    {
        var (keyStart, num) = ParseKeyOffset(line);
        return new OptSortEntry(line, keyStart, num);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  V2-OPTIMIZED — PIPELINE + ZERO-ALLOCATION + DEFLATE
// ═══════════════════════════════════════════════════════════════════════════════
public sealed class OptimizedPipelineDeflateSortAlgorithm : ISortAlgorithm
{
    public string Name        => "V2 Optimized Pipeline+Deflate";
    public string Description => "V2 with zero-allocation offsets, synchronous background I/O pipeline, struct priority queue.";

    private const long CHUNK = 512L * 1024 * 1024;
    private const int  RBUF  = 4   * 1024 * 1024;
    private const int  WBUF  = 64  * 1024 * 1024;
    private const int  IBUF  = 32  * 1024 * 1024;
    private const int  PDEPTH = 1;

    public async Task SortAsync(string inputPath, string outputPath)
    {
        string tmp = AlgoHelper.MakeTempDir(outputPath);
        try
        {
            var runs = await BuildAsync(inputPath, tmp);
            await KMergeAsync(runs, outputPath);
        }
        finally { AlgoHelper.CleanUp(tmp); }
    }

    private async Task<List<string>> BuildAsync(string input, string tmp)
    {
        var rawCh = Channel.CreateBounded<List<string>>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var srtCh = Channel.CreateBounded<(OptSortEntry[] e, int n)>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var files = new List<string>();

        var rdr = Task.Run(async () =>
        {
            using var fs = new FileStream(input, FileMode.Open, FileAccess.Read, FileShare.Read, IBUF, FileOptions.SequentialScan);
            using var r = new StreamReader(fs, Encoding.UTF8, false, IBUF);
            var chunk = new List<string>(AlgoHelper.EstimateLines(CHUNK));
            long cb = 0;
            string? line;
            while ((line = r.ReadLine()) != null)
            {
                chunk.Add(line);
                cb += line.Length * 2 + 56;
                if (cb >= CHUNK)
                {
                    await rawCh.Writer.WriteAsync(chunk).ConfigureAwait(false);
                    chunk = new(chunk.Count);
                    cb = 0;
                }
            }
            if (chunk.Count > 0) await rawCh.Writer.WriteAsync(chunk).ConfigureAwait(false);
            rawCh.Writer.Complete();
        });

        var srt = Task.Run(async () =>
        {
            await foreach (var raw in rawCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                var e = new OptSortEntry[raw.Count];
                for (int i = 0; i < raw.Count; i++) e[i] = OptLineParser.ParseEntry(raw[i]);
                Array.Sort(e, 0, raw.Count, OptSortEntryComparer.Instance);
                await srtCh.Writer.WriteAsync((e, raw.Count)).ConfigureAwait(false);
            }
            srtCh.Writer.Complete();
        });

        var wrt = Task.Run(async () =>
        {
            int idx = 0;
            await foreach (var (e, n) in srtCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                string p = Path.Combine(tmp, $"r{idx++:D6}.bin");
                WriteDeflate(p, e, n);
                files.Add(p);
            }
        });

        await Task.WhenAll(rdr, srt, wrt).ConfigureAwait(false);
        return files;
    }

    private void WriteDeflate(string path, OptSortEntry[] e, int n)
    {
        using var fs  = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        using var dfl = new DeflateStream(fs, CompressionLevel.Fastest, leaveOpen: true);
        using var wtr = new StreamWriter(dfl, Encoding.UTF8, WBUF);
        for (int i = 0; i < n; i++) wtr.WriteLine(e[i].Raw);
    }

    private StreamReader OpenDeflate(string path)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, RBUF, FileOptions.SequentialScan);
        return new StreamReader(new DeflateStream(fs, CompressionMode.Decompress), Encoding.UTF8, false, RBUF);
    }

    private Task KMergeAsync(List<string> paths, string output)
    {
        return Task.Run(() =>
        {
            int k = paths.Count;
            var rdrs = new StreamReader[k];
            try
            {
                for (int i = 0; i < k; i++) rdrs[i] = OpenDeflate(paths[i]);
                var heap = new PriorityQueue<OptMergeEntry, OptMergeEntry>(k, OptMergeEntryComparer.Instance);
                for (int i = 0; i < k; i++)
                {
                    var l = rdrs[i].ReadLine();
                    if (l != null)
                    {
                        var (keyStart, num) = OptLineParser.ParseKeyOffset(l);
                        var entry = new OptMergeEntry(l, keyStart, num, i);
                        heap.Enqueue(entry, entry);
                    }
                }
                using var ofs = new FileStream(output, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
                using var wtr = new StreamWriter(ofs, Encoding.UTF8, WBUF);
                while (heap.Count > 0)
                {
                    var item = heap.Dequeue();
                    wtr.WriteLine(item.Line);
                    var nx = rdrs[item.ReaderIndex].ReadLine();
                    if (nx != null)
                    {
                        var (keyStart, num) = OptLineParser.ParseKeyOffset(nx);
                        var entry = new OptMergeEntry(nx, keyStart, num, item.ReaderIndex);
                        heap.Enqueue(entry, entry);
                    }
                }
            }
            finally
            {
                for (int i = 0; i < k; i++)
                {
                    if (rdrs[i] != null)
                    {
                        rdrs[i].Dispose();
                        try { File.Delete(paths[i]); } catch { }
                    }
                }
            }
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  V3-OPTIMIZED — LZ4 + ZERO-ALLOCATION + PARALLEL MERGE TREE
// ═══════════════════════════════════════════════════════════════════════════════
public sealed class OptimizedLz4ParallelSortAlgorithm : ISortAlgorithm
{
    public string Name        => "V3 Optimized LZ4+ParallelSort";
    public string Description => "V3 with zero-allocation offsets, Partitioner range loops, Parallel Merge Tree, synchronous LZ4 writes.";

    private const long CHUNK  = 512L * 1024 * 1024;
    private const int  RBUF   = 4   * 1024 * 1024;
    private const int  WBUF   = 64  * 1024 * 1024;
    private const int  IBUF   = 32  * 1024 * 1024;
    private const int  PDEPTH = 1;
    private static readonly int P = Math.Max(1, Environment.ProcessorCount);

    public async Task SortAsync(string inputPath, string outputPath)
    {
        string tmp = AlgoHelper.MakeTempDir(outputPath);
        try
        {
            var runs = await BuildAsync(inputPath, tmp);
            await KMergeAsync(runs, outputPath);
        }
        finally { AlgoHelper.CleanUp(tmp); }
    }

    private async Task<List<string>> BuildAsync(string input, string tmp)
    {
        var rawCh = Channel.CreateBounded<List<string>>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var srtCh = Channel.CreateBounded<(OptSortEntry[] e, int n)>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var files = new List<string>();

        var rdr = Task.Run(async () =>
        {
            using var fs = new FileStream(input, FileMode.Open, FileAccess.Read, FileShare.Read, IBUF, FileOptions.SequentialScan);
            using var r = new StreamReader(fs, Encoding.UTF8, false, IBUF);
            var chunk = new List<string>(AlgoHelper.EstimateLines(CHUNK));
            long cb = 0;
            string? line;
            while ((line = r.ReadLine()) != null)
            {
                chunk.Add(line);
                cb += line.Length * 2 + 56;
                if (cb >= CHUNK)
                {
                    await rawCh.Writer.WriteAsync(chunk).ConfigureAwait(false);
                    chunk = new(chunk.Count);
                    cb = 0;
                }
            }
            if (chunk.Count > 0) await rawCh.Writer.WriteAsync(chunk).ConfigureAwait(false);
            rawCh.Writer.Complete();
        });

        var srt = Task.Run(async () =>
        {
            await foreach (var raw in rawCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                int count = raw.Count;
                var e = new OptSortEntry[count];
                Parallel.ForEach(System.Collections.Concurrent.Partitioner.Create(0, count), range =>
                {
                    for (int i = range.Item1; i < range.Item2; i++)
                    {
                        e[i] = OptLineParser.ParseEntry(raw[i]);
                    }
                });
                ParallelSort(e, count);
                await srtCh.Writer.WriteAsync((e, count)).ConfigureAwait(false);
            }
            srtCh.Writer.Complete();
        });

        var wrt = Task.Run(async () =>
        {
            int idx = 0;
            await foreach (var (e, n) in srtCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                string p = Path.Combine(tmp, $"r{idx++:D6}.lz4");
                WriteLz4(p, e, n);
                files.Add(p);
            }
        });

        await Task.WhenAll(rdr, srt, wrt).ConfigureAwait(false);
        return files;
    }

    private static void ParallelSort(OptSortEntry[] entries, int count)
    {
        var scratch = new OptSortEntry[count];
        ParallelMergeSort(entries, scratch, 0, count, P);
    }

    private static void ParallelMergeSort(OptSortEntry[] entries, OptSortEntry[] scratch, int start, int length, int degreeOfParallelism)
    {
        if (degreeOfParallelism <= 1 || length < 4096)
        {
            Array.Sort(entries, start, length, OptSortEntryComparer.Instance);
            return;
        }

        int mid = length / 2;
        int leftDeg = degreeOfParallelism / 2;
        int rightDeg = degreeOfParallelism - leftDeg;

        Parallel.Invoke(
            () => ParallelMergeSort(entries, scratch, start, mid, leftDeg),
            () => ParallelMergeSort(entries, scratch, start + mid, length - mid, rightDeg)
        );

        Merge(entries, start, mid, entries, start + mid, length - mid, scratch, start);
        Array.Copy(scratch, start, entries, start, length);
    }

    private static void Merge(
        OptSortEntry[] src1, int start1, int len1,
        OptSortEntry[] src2, int start2, int len2,
        OptSortEntry[] dest, int destStart)
    {
        int k = destStart;
        int end1 = start1 + len1, end2 = start2 + len2;
        var comparer = OptSortEntryComparer.Instance;

        while (start1 < end1 && start2 < end2)
        {
            dest[k++] = comparer.Compare(src1[start1], src2[start2]) <= 0 
                ? src1[start1++] 
                : src2[start2++];
        }

        if (start1 < end1) Array.Copy(src1, start1, dest, k, end1 - start1);
        else if (start2 < end2) Array.Copy(src2, start2, dest, k, end2 - start2);
    }

    private static void WriteLz4(string path, OptSortEntry[] e, int n)
    {
        using var fs  = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        using var lz4 = LZ4Stream.Encode(fs, K4os.Compression.LZ4.LZ4Level.L00_FAST, leaveOpen: true);
        using var wtr = new StreamWriter(lz4, Encoding.UTF8, WBUF);
        for (int i = 0; i < n; i++) wtr.WriteLine(e[i].Raw);
    }

    private static StreamReader OpenLz4(string path)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, RBUF, FileOptions.SequentialScan);
        return new StreamReader(LZ4Stream.Decode(fs, leaveOpen: false), Encoding.UTF8, false, RBUF);
    }

    private Task KMergeAsync(List<string> paths, string output)
    {
        return Task.Run(() =>
        {
            int k = paths.Count;
            var rdrs = new StreamReader[k];
            try
            {
                for (int i = 0; i < k; i++) rdrs[i] = OpenLz4(paths[i]);
                var heap = new PriorityQueue<OptMergeEntry, OptMergeEntry>(k, OptMergeEntryComparer.Instance);
                for (int i = 0; i < k; i++)
                {
                    var l = rdrs[i].ReadLine();
                    if (l != null)
                    {
                        var (keyStart, num) = OptLineParser.ParseKeyOffset(l);
                        var entry = new OptMergeEntry(l, keyStart, num, i);
                        heap.Enqueue(entry, entry);
                    }
                }
                using var ofs = new FileStream(output, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
                using var wtr = new StreamWriter(ofs, Encoding.UTF8, WBUF);
                while (heap.Count > 0)
                {
                    var item = heap.Dequeue();
                    wtr.WriteLine(item.Line);
                    var nx = rdrs[item.ReaderIndex].ReadLine();
                    if (nx != null)
                    {
                        var (keyStart, num) = OptLineParser.ParseKeyOffset(nx);
                        var entry = new OptMergeEntry(nx, keyStart, num, item.ReaderIndex);
                        heap.Enqueue(entry, entry);
                    }
                }
            }
            finally
            {
                for (int i = 0; i < k; i++)
                {
                    if (rdrs[i] != null)
                    {
                        rdrs[i].Dispose();
                        try { File.Delete(paths[i]); } catch { }
                    }
                }
            }
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  V4 INFRASTRUCTURE (Rank-Packed String-to-Integer Sorting)
// ═══════════════════════════════════════════════════════════════════════════════

internal readonly struct RankMergeEntry
{
    public readonly string Line;
    public readonly int KeyStartIndex;
    public readonly int Number;
    public readonly int ReaderIndex;

    public RankMergeEntry(string line, int keyStartIndex, int number, int readerIndex)
    {
        Line = line;
        KeyStartIndex = keyStartIndex;
        Number = number;
        ReaderIndex = readerIndex;
    }
}

internal sealed class RankMergeEntryComparer : IComparer<RankMergeEntry>
{
    public static readonly RankMergeEntryComparer Instance = new();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(RankMergeEntry x, RankMergeEntry y)
    {
        int c = x.Line.AsSpan(x.KeyStartIndex).CompareTo(y.Line.AsSpan(y.KeyStartIndex), StringComparison.OrdinalIgnoreCase);
        return c != 0 ? c : x.Number.CompareTo(y.Number);
    }
}

internal static class RankLineParser
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static (int keyStart, int num) ParseKeyOffset(string line)
    {
        int num = 0;
        int len = line.Length;
        for (int i = 0; i < len; i++)
        {
            char c = line[i];
            if (c == '.')
            {
                int ts = i + 1;
                if (ts < len && line[ts] == ' ') ts++;
                return (ts, num);
            }
            uint d = (uint)(c - '0');
            if (d <= 9) num = num * 10 + (int)d;
        }
        return (0, 0);
    }

    public static void ProcessChunk(
        List<string> raw, long[] keys, string[] lines, int count)
    {
        var unique = new List<string>(64);

        // First pass: Parse offsets and numbers and extract unique strings
        for (int i = 0; i < count; i++)
        {
            string line = raw[i];
            lines[i] = line;
            var (keyStart, num) = ParseKeyOffset(line);

            var keySpan = line.AsSpan(keyStart);
            int rank = -1;
            for (int j = 0; j < unique.Count; j++)
            {
                if (keySpan.Equals(unique[j], StringComparison.OrdinalIgnoreCase))
                {
                    rank = j;
                    break;
                }
            }
            if (rank == -1)
            {
                unique.Add(new string(keySpan));
            }
        }

        // Sort unique strings alphabetically to determine final ranks
        unique.Sort(StringComparer.OrdinalIgnoreCase);

        // Second pass: Bit-pack keys using the sorted ranks
        for (int i = 0; i < count; i++)
        {
            string line = lines[i];
            var (keyStart, num) = ParseKeyOffset(line);
            var keySpan = line.AsSpan(keyStart);

            int rank = 0;
            for (int j = 0; j < unique.Count; j++)
            {
                if (keySpan.Equals(unique[j], StringComparison.OrdinalIgnoreCase))
                {
                    rank = j;
                    break;
                }
            }

            keys[i] = ((long)rank << 32) | (uint)num;
        }
    }

    public static void ProcessChunkParallel(
        List<string> raw, long[] keys, string[] lines, int count)
    {
        var unique = new List<string>(64);
        var offsets = new int[count];
        var numbers = new int[count];

        // 1. Parallel Parse
        Parallel.ForEach(System.Collections.Concurrent.Partitioner.Create(0, count), range =>
        {
            for (int i = range.Item1; i < range.Item2; i++)
            {
                lines[i] = raw[i];
                var (keyStart, num) = ParseKeyOffset(raw[i]);
                offsets[i] = keyStart;
                numbers[i] = num;
            }
        });

        // 2. Discover Unique Strings (sequential since count is small and it's fast)
        for (int i = 0; i < count; i++)
        {
            var keySpan = lines[i].AsSpan(offsets[i]);
            int rank = -1;
            for (int j = 0; j < unique.Count; j++)
            {
                if (keySpan.Equals(unique[j], StringComparison.OrdinalIgnoreCase))
                {
                    rank = j;
                    break;
                }
            }
            if (rank == -1)
            {
                unique.Add(new string(keySpan));
            }
        }

        // 3. Sort unique strings alphabetically
        unique.Sort(StringComparer.OrdinalIgnoreCase);

        // 4. Parallel Bit-packing
        Parallel.ForEach(System.Collections.Concurrent.Partitioner.Create(0, count), range =>
        {
            for (int i = range.Item1; i < range.Item2; i++)
            {
                var keySpan = lines[i].AsSpan(offsets[i]);
                int rank = 0;
                for (int j = 0; j < unique.Count; j++)
                {
                    if (keySpan.Equals(unique[j], StringComparison.OrdinalIgnoreCase))
                    {
                        rank = j;
                        break;
                    }
                }
                keys[i] = ((long)rank << 32) | (uint)numbers[i];
            }
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  V4-DEFLATE — RANK-PACKED + PIPELINE + DEFLATE
// ═══════════════════════════════════════════════════════════════════════════════
public sealed class RankPackingDeflateSortAlgorithm : ISortAlgorithm
{
    public string Name        => "V4 Rank-Packed Pipeline+Deflate";
    public string Description => "V4 with Rank-Packed long keys (alphabetical rank mapping), synchronous background I/O, struct priority queue.";

    private const long CHUNK = 512L * 1024 * 1024;
    private const int  RBUF  = 4   * 1024 * 1024;
    private const int  WBUF  = 64  * 1024 * 1024;
    private const int  IBUF  = 32  * 1024 * 1024;
    private const int  PDEPTH = 1;

    public async Task SortAsync(string inputPath, string outputPath)
    {
        string tmp = AlgoHelper.MakeTempDir(outputPath);
        try
        {
            var runs = await BuildAsync(inputPath, tmp);
            await KMergeAsync(runs, outputPath);
        }
        finally { AlgoHelper.CleanUp(tmp); }
    }

    private async Task<List<string>> BuildAsync(string input, string tmp)
    {
        var rawCh = Channel.CreateBounded<List<string>>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var srtCh = Channel.CreateBounded<(long[] k, string[] l, int n)>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var files = new List<string>();

        var rdr = Task.Run(async () =>
        {
            using var fs = new FileStream(input, FileMode.Open, FileAccess.Read, FileShare.Read, IBUF, FileOptions.SequentialScan);
            using var r = new StreamReader(fs, Encoding.UTF8, false, IBUF);
            var chunk = new List<string>(AlgoHelper.EstimateLines(CHUNK));
            long cb = 0;
            string? line;
            while ((line = r.ReadLine()) != null)
            {
                chunk.Add(line);
                cb += line.Length * 2 + 56;
                if (cb >= CHUNK)
                {
                    await rawCh.Writer.WriteAsync(chunk).ConfigureAwait(false);
                    chunk = new(chunk.Count);
                    cb = 0;
                }
            }
            if (chunk.Count > 0) await rawCh.Writer.WriteAsync(chunk).ConfigureAwait(false);
            rawCh.Writer.Complete();
        });

        var srt = Task.Run(async () =>
        {
            await foreach (var raw in rawCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                int count = raw.Count;
                var keys = new long[count];
                var lines = new string[count];

                // Synchronous chunk processing (avoiding .NET 9 & ref struct async restrictions)
                RankLineParser.ProcessChunk(raw, keys, lines, count);

                // Sort using primitive long sorting
                Array.Sort(keys, lines, 0, count);

                await srtCh.Writer.WriteAsync((keys, lines, count)).ConfigureAwait(false);
            }
            srtCh.Writer.Complete();
        });

        var wrt = Task.Run(async () =>
        {
            int idx = 0;
            await foreach (var (_, l, n) in srtCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                string p = Path.Combine(tmp, $"r{idx++:D6}.bin");
                WriteDeflate(p, l, n);
                files.Add(p);
            }
        });

        await Task.WhenAll(rdr, srt, wrt).ConfigureAwait(false);
        return files;
    }

    private void WriteDeflate(string path, string[] lines, int n)
    {
        using var fs  = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        using var dfl = new DeflateStream(fs, CompressionLevel.Fastest, leaveOpen: true);
        using var wtr = new StreamWriter(dfl, Encoding.UTF8, WBUF);
        for (int i = 0; i < n; i++) wtr.WriteLine(lines[i]);
    }

    private StreamReader OpenDeflate(string path)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, RBUF, FileOptions.SequentialScan);
        return new StreamReader(new DeflateStream(fs, CompressionMode.Decompress), Encoding.UTF8, false, RBUF);
    }

    private Task KMergeAsync(List<string> paths, string output)
    {
        return Task.Run(() =>
        {
            int k = paths.Count;
            var rdrs = new StreamReader[k];
            try
            {
                for (int i = 0; i < k; i++) rdrs[i] = OpenDeflate(paths[i]);
                var heap = new PriorityQueue<RankMergeEntry, RankMergeEntry>(k, RankMergeEntryComparer.Instance);
                for (int i = 0; i < k; i++)
                {
                    var l = rdrs[i].ReadLine();
                    if (l != null)
                    {
                        var (keyStart, num) = RankLineParser.ParseKeyOffset(l);
                        var entry = new RankMergeEntry(l, keyStart, num, i);
                        heap.Enqueue(entry, entry);
                    }
                }
                using var ofs = new FileStream(output, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
                using var wtr = new StreamWriter(ofs, Encoding.UTF8, WBUF);
                while (heap.Count > 0)
                {
                    var item = heap.Dequeue();
                    wtr.WriteLine(item.Line);
                    var nx = rdrs[item.ReaderIndex].ReadLine();
                    if (nx != null)
                    {
                        var (keyStart, num) = RankLineParser.ParseKeyOffset(nx);
                        var entry = new RankMergeEntry(nx, keyStart, num, item.ReaderIndex);
                        heap.Enqueue(entry, entry);
                    }
                }
            }
            finally
            {
                for (int i = 0; i < k; i++)
                {
                    if (rdrs[i] != null)
                    {
                        rdrs[i].Dispose();
                        try { File.Delete(paths[i]); } catch { }
                    }
                }
            }
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  V4-LZ4 — RANK-PACKED + PIPELINE + PARALLEL MERGE TREE + LZ4
// ═══════════════════════════════════════════════════════════════════════════════
public sealed class OptimizedLz4RankSortAlgorithm : ISortAlgorithm
{
    public string Name        => "V4 Optimized LZ4+RankSort";
    public string Description => "V4 with Rank-Packed long keys, Parallel synchronous parsing/ranking, Parallel primitive Merge Tree, synchronous LZ4 writes.";

    private const long CHUNK  = 512L * 1024 * 1024;
    private const int  RBUF   = 4   * 1024 * 1024;
    private const int  WBUF   = 64  * 1024 * 1024;
    private const int  IBUF   = 32  * 1024 * 1024;
    private const int  PDEPTH = 1;
    private static readonly int P = Math.Max(1, Environment.ProcessorCount);

    public async Task SortAsync(string inputPath, string outputPath)
    {
        string tmp = AlgoHelper.MakeTempDir(outputPath);
        try
        {
            var runs = await BuildAsync(inputPath, tmp);
            await KMergeAsync(runs, outputPath);
        }
        finally { AlgoHelper.CleanUp(tmp); }
    }

    private async Task<List<string>> BuildAsync(string input, string tmp)
    {
        var rawCh = Channel.CreateBounded<List<string>>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var srtCh = Channel.CreateBounded<(long[] k, string[] l, int n)>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var files = new List<string>();

        var rdr = Task.Run(async () =>
        {
            using var fs = new FileStream(input, FileMode.Open, FileAccess.Read, FileShare.Read, IBUF, FileOptions.SequentialScan);
            using var r = new StreamReader(fs, Encoding.UTF8, false, IBUF);
            var chunk = new List<string>(AlgoHelper.EstimateLines(CHUNK));
            long cb = 0;
            string? line;
            while ((line = r.ReadLine()) != null)
            {
                chunk.Add(line);
                cb += line.Length * 2 + 56;
                if (cb >= CHUNK)
                {
                    await rawCh.Writer.WriteAsync(chunk).ConfigureAwait(false);
                    chunk = new(chunk.Count);
                    cb = 0;
                }
            }
            if (chunk.Count > 0) await rawCh.Writer.WriteAsync(chunk).ConfigureAwait(false);
            rawCh.Writer.Complete();
        });

        var srt = Task.Run(async () =>
        {
            await foreach (var raw in rawCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                int count = raw.Count;
                var keys = new long[count];
                var lines = new string[count];

                // Parallel synchronous parsing and ranking
                RankLineParser.ProcessChunkParallel(raw, keys, lines, count);

                // Custom Parallel Primitive Merge Sort
                ParallelSort(keys, lines, count);

                await srtCh.Writer.WriteAsync((keys, lines, count)).ConfigureAwait(false);
            }
            srtCh.Writer.Complete();
        });

        var wrt = Task.Run(async () =>
        {
            int idx = 0;
            await foreach (var (_, l, n) in srtCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                string p = Path.Combine(tmp, $"r{idx++:D6}.lz4");
                WriteLz4(p, l, n);
                files.Add(p);
            }
        });

        await Task.WhenAll(rdr, srt, wrt).ConfigureAwait(false);
        return files;
    }

    private static void ParallelSort(long[] keys, string[] lines, int count)
    {
        var scratchKeys = new long[count];
        var scratchLines = new string[count];
        ParallelMergeSort(keys, lines, scratchKeys, scratchLines, 0, count, P);
    }

    private static void ParallelMergeSort(
        long[] keys, string[] lines,
        long[] scratchKeys, string[] scratchLines,
        int start, int length, int degreeOfParallelism)
    {
        if (degreeOfParallelism <= 1 || length < 4096)
        {
            Array.Sort(keys, lines, start, length);
            return;
        }

        int mid = length / 2;
        int leftDeg = degreeOfParallelism / 2;
        int rightDeg = degreeOfParallelism - leftDeg;

        Parallel.Invoke(
            () => ParallelMergeSort(keys, lines, scratchKeys, scratchLines, start, mid, leftDeg),
            () => ParallelMergeSort(keys, lines, scratchKeys, scratchLines, start + mid, length - mid, rightDeg)
        );

        Merge(keys, lines, start, mid, keys, lines, start + mid, length - mid, scratchKeys, scratchLines, start);
        Array.Copy(scratchKeys, start, keys, start, length);
        Array.Copy(scratchLines, start, lines, start, length);
    }

    private static void Merge(
        long[] srcKeys1, string[] srcLines1, int start1, int len1,
        long[] srcKeys2, string[] srcLines2, int start2, int len2,
        long[] destKeys, string[] destLines, int destStart)
    {
        int k = destStart;
        int end1 = start1 + len1, end2 = start2 + len2;

        while (start1 < end1 && start2 < end2)
        {
            if (srcKeys1[start1] <= srcKeys2[start2])
            {
                destKeys[k] = srcKeys1[start1];
                destLines[k++] = srcLines1[start1++];
            }
            else
            {
                destKeys[k] = srcKeys2[start2];
                destLines[k++] = srcLines2[start2++];
            }
        }

        if (start1 < end1)
        {
            int rem = end1 - start1;
            Array.Copy(srcKeys1, start1, destKeys, k, rem);
            Array.Copy(srcLines1, start1, destLines, k, rem);
        }
        else if (start2 < end2)
        {
            int rem = end2 - start2;
            Array.Copy(srcKeys2, start2, destKeys, k, rem);
            Array.Copy(srcLines2, start2, destLines, k, rem);
        }
    }

    private static void WriteLz4(string path, string[] lines, int n)
    {
        using var fs  = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        using var lz4 = LZ4Stream.Encode(fs, K4os.Compression.LZ4.LZ4Level.L00_FAST, leaveOpen: true);
        using var wtr = new StreamWriter(lz4, Encoding.UTF8, WBUF);
        for (int i = 0; i < n; i++) wtr.WriteLine(lines[i]);
    }

    private static StreamReader OpenLz4(string path)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, RBUF, FileOptions.SequentialScan);
        return new StreamReader(LZ4Stream.Decode(fs, leaveOpen: false), Encoding.UTF8, false, RBUF);
    }

    private Task KMergeAsync(List<string> paths, string output)
    {
        return Task.Run(() =>
        {
            int k = paths.Count;
            var rdrs = new StreamReader[k];
            try
            {
                for (int i = 0; i < k; i++) rdrs[i] = OpenLz4(paths[i]);
                var heap = new PriorityQueue<RankMergeEntry, RankMergeEntry>(k, RankMergeEntryComparer.Instance);
                for (int i = 0; i < k; i++)
                {
                    var l = rdrs[i].ReadLine();
                    if (l != null)
                    {
                        var (keyStart, num) = RankLineParser.ParseKeyOffset(l);
                        var entry = new RankMergeEntry(l, keyStart, num, i);
                        heap.Enqueue(entry, entry);
                    }
                }
                using var ofs = new FileStream(output, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
                using var wtr = new StreamWriter(ofs, Encoding.UTF8, WBUF);
                while (heap.Count > 0)
                {
                    var item = heap.Dequeue();
                    wtr.WriteLine(item.Line);
                    var nx = rdrs[item.ReaderIndex].ReadLine();
                    if (nx != null)
                    {
                        var (keyStart, num) = RankLineParser.ParseKeyOffset(nx);
                        var entry = new RankMergeEntry(nx, keyStart, num, item.ReaderIndex);
                        heap.Enqueue(entry, entry);
                    }
                }
            }
            finally
            {
                for (int i = 0; i < k; i++)
                {
                    if (rdrs[i] != null)
                    {
                        rdrs[i].Dispose();
                        try { File.Delete(paths[i]); } catch { }
                    }
                }
            }
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  V5 EXTREME — ZERO-ALLOCATION BYTE-BUFFER + LZ4 + PARALLEL MERGE
// ═══════════════════════════════════════════════════════════════════════════════
public sealed class ZeroAllocLz4ByteSortAlgorithm : ISortAlgorithm
{
    public string Name        => "V5 Zero-Allocation LZ4+ByteSort";
    public string Description => "V5 with zero-allocation single-pass byte buffer parsing, direct byte chunk copying, parallel long/int array sort, and LZ4 temp files.";

    private const long CHUNK  = 512L * 1024 * 1024;
    private const int  RBUF   = 4   * 1024 * 1024;
    private const int  WBUF   = 64  * 1024 * 1024;
    private const int  IBUF   = 32  * 1024 * 1024;
    private const int  PDEPTH = 1;
    private static readonly int P = Math.Max(1, Environment.ProcessorCount);

    public async Task SortAsync(string inputPath, string outputPath)
    {
        string tmp = AlgoHelper.MakeTempDir(outputPath);
        try
        {
            var runs = await BuildAsync(inputPath, tmp);
            await KMergeAsync(runs, outputPath);
        }
        finally { AlgoHelper.CleanUp(tmp); }
    }

    public sealed class ByteChunk
    {
        public readonly byte[] Buffer;
        public readonly int Length;

        public ByteChunk(byte[] buffer, int length)
        {
            Buffer = buffer;
            Length = length;
        }
    }

    private async Task<List<string>> BuildAsync(string input, string tmp)
    {
        var rawCh = Channel.CreateBounded<ByteChunk>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var srtCh = Channel.CreateBounded<(byte[] buf, int[] starts, int[] lens, int[] indices, int n)>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = true });
        var files = new List<string>();

        var rdr = Task.Run(async () =>
        {
            using var fs = new FileStream(input, FileMode.Open, FileAccess.Read, FileShare.Read, IBUF, FileOptions.SequentialScan);
            byte[] leftover = new byte[1024 * 1024]; // 1 MB overflow area
            int leftoverLen = 0;

            while (true)
            {
                byte[] buffer = new byte[CHUNK + 1024 * 1024];
                int bytesRead = 0;

                if (leftoverLen > 0)
                {
                    Buffer.BlockCopy(leftover, 0, buffer, 0, leftoverLen);
                    bytesRead = leftoverLen;
                    leftoverLen = 0;
                }

                int targetRead = (int)CHUNK - bytesRead;
                if (targetRead > 0)
                {
                    int read = fs.Read(buffer, bytesRead, targetRead);
                    bytesRead += read;
                }

                if (bytesRead == 0) break;

                int end = bytesRead;
                while (end > 0 && buffer[end - 1] != (byte)'\n')
                {
                    end--;
                }

                if (end == 0) end = bytesRead;

                int leftoverSize = bytesRead - end;
                if (leftoverSize > 0)
                {
                    Buffer.BlockCopy(buffer, end, leftover, 0, leftoverSize);
                    leftoverLen = leftoverSize;
                }

                await rawCh.Writer.WriteAsync(new ByteChunk(buffer, end)).ConfigureAwait(false);
            }
            rawCh.Writer.Complete();
        });

        var srt = Task.Run(async () =>
        {
            await foreach (var chunk in rawCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                // Synchronous chunk processing (avoiding .NET 9 & ref struct async restrictions)
                var (keys, starts, lens, indices, lineIdx) = ProcessChunk(chunk);

                // Parallel sort indices and primitive long keys in parallel
                ParallelSort(keys, indices, lineIdx);

                await srtCh.Writer.WriteAsync((chunk.Buffer, starts, lens, indices, lineIdx)).ConfigureAwait(false);
            }
            srtCh.Writer.Complete();
        });

        var wrt = Task.Run(async () =>
        {
            int idx = 0;
            await foreach (var (buf, starts, lens, indices, n) in srtCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                string p = Path.Combine(tmp, $"r{idx++:D6}.lz4");
                WriteLz4(p, buf, starts, lens, indices, n);
                files.Add(p);
            }
        });

        await Task.WhenAll(rdr, srt, wrt).ConfigureAwait(false);
        return files;
    }

    private static (long[] keys, int[] starts, int[] lens, int[] indices, int count) ProcessChunk(ByteChunk chunk)
    {
        int capacity = chunk.Length / 30; // Estimated line count
        int[] lineStarts = new int[capacity];
        int[] lineLengths = new int[capacity];
        int[] rawRanks = new int[capacity];
        int[] numbers = new int[capacity];

        int lineIdx = 0;
        int currStart = 0;
        var uniqueKeys = new List<byte[]>(32);

        for (int i = 0; i < chunk.Length; i++)
        {
            if (chunk.Buffer[i] == (byte)'\n')
            {
                int len = i - currStart + 1;
                if (lineIdx >= lineStarts.Length)
                {
                    int newCap = lineStarts.Length * 2;
                    Array.Resize(ref lineStarts, newCap);
                    Array.Resize(ref lineLengths, newCap);
                    Array.Resize(ref rawRanks, newCap);
                    Array.Resize(ref numbers, newCap);
                }

                lineStarts[lineIdx] = currStart;
                lineLengths[lineIdx] = len;

                var lineSpan = chunk.Buffer.AsSpan(currStart, len);
                int parseLen = len;
                while (parseLen > 0 && (lineSpan[parseLen - 1] == (byte)'\n' || lineSpan[parseLen - 1] == (byte)'\r'))
                {
                    parseLen--;
                }
                var parseSpan = lineSpan.Slice(0, parseLen);

                var (keyStart, num) = ParseKeyOffset(parseSpan);
                var keySpan = parseSpan.Slice(keyStart);

                int rawRank = -1;
                for (int j = 0; j < uniqueKeys.Count; j++)
                {
                    if (EqualsIgnoreCase(keySpan, uniqueKeys[j]))
                    {
                        rawRank = j;
                        break;
                    }
                }
                if (rawRank == -1)
                {
                    byte[] newKey = keySpan.ToArray();
                    rawRank = uniqueKeys.Count;
                    uniqueKeys.Add(newKey);
                }

                rawRanks[lineIdx] = rawRank;
                numbers[lineIdx] = num;

                lineIdx++;
                currStart = i + 1;
            }
        }

        if (currStart < chunk.Length)
        {
            int len = chunk.Length - currStart;
            if (lineIdx >= lineStarts.Length)
            {
                int newCap = lineStarts.Length * 2;
                Array.Resize(ref lineStarts, newCap);
                Array.Resize(ref lineLengths, newCap);
                Array.Resize(ref rawRanks, newCap);
                Array.Resize(ref numbers, newCap);
            }

            lineStarts[lineIdx] = currStart;
            lineLengths[lineIdx] = len;

            var lineSpan = chunk.Buffer.AsSpan(currStart, len);
            int parseLen = len;
            while (parseLen > 0 && (lineSpan[parseLen - 1] == (byte)'\n' || lineSpan[parseLen - 1] == (byte)'\r'))
            {
                parseLen--;
            }
            var parseSpan = lineSpan.Slice(0, parseLen);

            var (keyStart, num) = ParseKeyOffset(parseSpan);
            var keySpan = parseSpan.Slice(keyStart);

            int rawRank = -1;
            for (int j = 0; j < uniqueKeys.Count; j++)
            {
                if (EqualsIgnoreCase(keySpan, uniqueKeys[j]))
                {
                    rawRank = j;
                    break;
                }
            }
            if (rawRank == -1)
            {
                byte[] newKey = keySpan.ToArray();
                rawRank = uniqueKeys.Count;
                uniqueKeys.Add(newKey);
            }

            rawRanks[lineIdx] = rawRank;
            numbers[lineIdx] = num;
            lineIdx++;
        }

        // Map raw distinct ranks to alphabetical rank indices
        var uniqueStrings = new List<string>(uniqueKeys.Count);
        var rawUniqueStrings = new List<string>(uniqueKeys.Count);
        for (int i = 0; i < uniqueKeys.Count; i++)
        {
            string s = Encoding.UTF8.GetString(uniqueKeys[i]);
            uniqueStrings.Add(s);
            rawUniqueStrings.Add(s);
        }
        uniqueStrings.Sort(StringComparer.OrdinalIgnoreCase);

        int[] rankMapping = new int[uniqueKeys.Count];
        for (int i = 0; i < uniqueKeys.Count; i++)
        {
            rankMapping[i] = uniqueStrings.IndexOf(rawUniqueStrings[i]);
        }

        long[] keys = new long[lineIdx];
        int[] indices = new int[lineIdx];

        Parallel.ForEach(System.Collections.Concurrent.Partitioner.Create(0, lineIdx), range =>
        {
            for (int i = range.Item1; i < range.Item2; i++)
            {
                int rank = rankMapping[rawRanks[i]];
                keys[i] = ((long)rank << 32) | (uint)numbers[i];
                indices[i] = i;
            }
        });

        return (keys, lineStarts, lineLengths, indices, lineIdx);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (int keyStart, int num) ParseKeyOffset(ReadOnlySpan<byte> line)
    {
        int num = 0;
        int len = line.Length;
        for (int i = 0; i < len; i++)
        {
            byte b = line[i];
            if (b == (byte)'.')
            {
                int ts = i + 1;
                if (ts < len && line[ts] == (byte)' ') ts++;
                return (ts, num);
            }
            uint d = (uint)(b - (byte)'0');
            if (d <= 9) num = num * 10 + (int)d;
        }
        return (0, 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EqualsIgnoreCase(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
        {
            byte c1 = a[i];
            byte c2 = b[i];
            if (c1 == c2) continue;
            if (c1 >= 65 && c1 <= 90) c1 += 32;
            if (c2 >= 65 && c2 <= 90) c2 += 32;
            if (c1 != c2) return false;
        }
        return true;
    }

    private static void ParallelSort(long[] keys, int[] indices, int count)
    {
        var scratchKeys = new long[count];
        var scratchIndices = new int[count];
        ParallelMergeSort(keys, indices, scratchKeys, scratchIndices, 0, count, P);
    }

    private static void ParallelMergeSort(
        long[] keys, int[] indices,
        long[] scratchKeys, int[] scratchIndices,
        int start, int length, int degreeOfParallelism)
    {
        if (degreeOfParallelism <= 1 || length < 4096)
        {
            Array.Sort(keys, indices, start, length);
            return;
        }

        int mid = length / 2;
        int leftDeg = degreeOfParallelism / 2;
        int rightDeg = degreeOfParallelism - leftDeg;

        Parallel.Invoke(
            () => ParallelMergeSort(keys, indices, scratchKeys, scratchIndices, start, mid, leftDeg),
            () => ParallelMergeSort(keys, indices, scratchKeys, scratchIndices, start + mid, length - mid, rightDeg)
        );

        Merge(keys, indices, start, mid, keys, indices, start + mid, length - mid, scratchKeys, scratchIndices, start);
        Array.Copy(scratchKeys, start, keys, start, length);
        Array.Copy(scratchIndices, start, indices, start, length);
    }

    private static void Merge(
        long[] srcKeys1, int[] srcIndices1, int start1, int len1,
        long[] srcKeys2, int[] srcIndices2, int start2, int len2,
        long[] destKeys, int[] destIndices, int destStart)
    {
        int k = destStart;
        int end1 = start1 + len1, end2 = start2 + len2;

        while (start1 < end1 && start2 < end2)
        {
            if (srcKeys1[start1] <= srcKeys2[start2])
            {
                destKeys[k] = srcKeys1[start1];
                destIndices[k++] = srcIndices1[start1++];
            }
            else
            {
                destKeys[k] = srcKeys2[start2];
                destIndices[k++] = srcIndices2[start2++];
            }
        }

        if (start1 < end1)
        {
            int rem = end1 - start1;
            Array.Copy(srcKeys1, start1, destKeys, k, rem);
            Array.Copy(srcIndices1, start1, destIndices, k, rem);
        }
        else if (start2 < end2)
        {
            int rem = end2 - start2;
            Array.Copy(srcKeys2, start2, destKeys, k, rem);
            Array.Copy(srcIndices2, start2, destIndices, k, rem);
        }
    }

    private static void WriteLz4(string path, byte[] buffer, int[] starts, int[] lens, int[] indices, int count)
    {
        using var fs  = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        using var lz4 = LZ4Stream.Encode(fs, K4os.Compression.LZ4.LZ4Level.L00_FAST, leaveOpen: true);
        
        byte[] writeBuffer = new byte[64 * 1024 * 1024];
        int writeOffset = 0;

        for (int i = 0; i < count; i++)
        {
            int idx = indices[i];
            int start = starts[idx];
            int len = lens[idx];

            if (writeOffset + len > writeBuffer.Length)
            {
                lz4.Write(writeBuffer, 0, writeOffset);
                writeOffset = 0;
            }

            Buffer.BlockCopy(buffer, start, writeBuffer, writeOffset, len);
            writeOffset += len;
        }

        if (writeOffset > 0)
        {
            lz4.Write(writeBuffer, 0, writeOffset);
        }
    }

    private static StreamReader OpenLz4(string path)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, RBUF, FileOptions.SequentialScan);
        return new StreamReader(LZ4Stream.Decode(fs, leaveOpen: false), Encoding.UTF8, false, RBUF);
    }

    private Task KMergeAsync(List<string> paths, string output)
    {
        return Task.Run(() =>
        {
            int k = paths.Count;
            var rdrs = new StreamReader[k];
            try
            {
                for (int i = 0; i < k; i++) rdrs[i] = OpenLz4(paths[i]);
                var heap = new PriorityQueue<RankMergeEntry, RankMergeEntry>(k, RankMergeEntryComparer.Instance);
                for (int i = 0; i < k; i++)
                {
                    var l = rdrs[i].ReadLine();
                    if (l != null)
                    {
                        var (keyStart, num) = RankLineParser.ParseKeyOffset(l);
                        var entry = new RankMergeEntry(l, keyStart, num, i);
                        heap.Enqueue(entry, entry);
                    }
                }
                using var ofs = new FileStream(output, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
                using var wtr = new StreamWriter(ofs, Encoding.UTF8, WBUF);
                while (heap.Count > 0)
                {
                    var item = heap.Dequeue();
                    wtr.WriteLine(item.Line);
                    var nx = rdrs[item.ReaderIndex].ReadLine();
                    if (nx != null)
                    {
                        var (keyStart, num) = RankLineParser.ParseKeyOffset(nx);
                        var entry = new RankMergeEntry(nx, keyStart, num, item.ReaderIndex);
                        heap.Enqueue(entry, entry);
                    }
                }
            }
            finally
            {
                for (int i = 0; i < k; i++)
                {
                    if (rdrs[i] != null)
                    {
                        rdrs[i].Dispose();
                        try { File.Delete(paths[i]); } catch { }
                    }
                }
            }
        });
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  V6 ZERO-ALLOCATION RADIX-PIPELINE + BYTE-LEVEL MERGE
// ═══════════════════════════════════════════════════════════════════════════════

public sealed class ByteRunReader : IDisposable
{
    private readonly Stream _stream;
    private readonly byte[] _buffer;
    private int _bufferStart;
    private int _bufferEnd;
    private bool _eof;

    public byte[] Buffer => _buffer;

    public ByteRunReader(string path, int bufferSize)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, FileOptions.SequentialScan);
        _stream = LZ4Stream.Decode(fs, leaveOpen: false);
        _buffer = new byte[bufferSize];
        _bufferStart = 0;
        _bufferEnd = 0;
        _eof = false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadLine(out int lineStart, out int lineLength)
    {
        lineStart = 0;
        lineLength = 0;

        while (true)
        {
            // Scan for newline in the current buffer
            int searchStart = _bufferStart;
            int newlineIdx = -1;
            for (int i = searchStart; i < _bufferEnd; i++)
            {
                if (_buffer[i] == (byte)'\n')
                {
                    newlineIdx = i;
                    break;
                }
            }

            if (newlineIdx != -1)
            {
                lineStart = _bufferStart;
                lineLength = newlineIdx - _bufferStart + 1;
                _bufferStart = newlineIdx + 1;
                return true;
            }

            // No newline found. If EOF, return whatever is left (if anything)
            if (_eof)
            {
                if (_bufferStart < _bufferEnd)
                {
                    lineStart = _bufferStart;
                    lineLength = _bufferEnd - _bufferStart;
                    _bufferStart = _bufferEnd;
                    return true;
                }
                return false;
            }

            // Compact buffer and read more
            int remaining = _bufferEnd - _bufferStart;
            if (remaining > 0)
            {
                System.Buffer.BlockCopy(_buffer, _bufferStart, _buffer, 0, remaining);
            }
            _bufferStart = 0;
            _bufferEnd = remaining;

            int read = _stream.Read(_buffer, _bufferEnd, _buffer.Length - _bufferEnd);
            if (read == 0)
            {
                _eof = true;
            }
            else
            {
                _bufferEnd += read;
            }
        }
    }

    public void Dispose()
    {
        _stream.Dispose();
    }
}

public readonly struct ByteMergeEntry
{
    public readonly byte[] Buffer;
    public readonly int KeyStart;
    public readonly int KeyLength;
    public readonly int Number;
    public readonly int LineStart;
    public readonly int LineLength;
    public readonly int ReaderIndex;

    public ByteMergeEntry(byte[] buffer, int keyStart, int keyLength, int number, int lineStart, int lineLength, int readerIndex)
    {
        Buffer = buffer;
        KeyStart = keyStart;
        KeyLength = keyLength;
        Number = number;
        LineStart = lineStart;
        LineLength = lineLength;
        ReaderIndex = readerIndex;
    }
}

internal sealed class ByteMergeEntryComparer : IComparer<ByteMergeEntry>
{
    public static readonly ByteMergeEntryComparer Instance = new();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ByteMergeEntry x, ByteMergeEntry y)
    {
        int c = EqualsIgnoreCaseCompare(
            x.Buffer.AsSpan(x.KeyStart, x.KeyLength),
            y.Buffer.AsSpan(y.KeyStart, y.KeyLength));
        return c != 0 ? c : x.Number.CompareTo(y.Number);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int EqualsIgnoreCaseCompare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int minLen = Math.Min(a.Length, b.Length);
        for (int i = 0; i < minLen; i++)
        {
            byte c1 = a[i];
            byte c2 = b[i];
            if (c1 == c2) continue;
            if (c1 >= 65 && c1 <= 90) c1 += 32;
            if (c2 >= 65 && c2 <= 90) c2 += 32;
            if (c1 != c2) return c1 - c2;
        }
        return a.Length - b.Length;
    }
}

internal static class RadixSorter
{
    public static void RadixSort(ulong[] arr, ulong[] temp, int count)
    {
        const int bits = 16;
        const int mask = (1 << bits) - 1;
        const int bucketsCount = 1 << bits;
        int[] counts = new int[bucketsCount];

        ulong[] source = arr;
        ulong[] dest = temp;

        for (int shift = 0; shift < 64; shift += bits)
        {
            Array.Clear(counts, 0, bucketsCount);

            for (int i = 0; i < count; i++)
            {
                int val = (int)((source[i] >> shift) & mask);
                counts[val]++;
            }

            int sum = 0;
            for (int i = 0; i < bucketsCount; i++)
            {
                int tempCount = counts[i];
                counts[i] = sum;
                sum += tempCount;
            }

            for (int i = 0; i < count; i++)
            {
                ulong val = source[i];
                int bucket = (int)((val >> shift) & mask);
                dest[counts[bucket]++] = val;
            }

            var t = source;
            source = dest;
            dest = t;
        }

        if (source != arr)
        {
            Array.Copy(source, 0, arr, 0, count);
        }
    }
}

public sealed class ZeroAllocRadixByteSortAlgorithm : ISortAlgorithm
{
    public string Name        => "V6 Zero-Allocation Radix-Pipeline + Byte-Level Merge";
    public string Description => "V6 with zero-allocation byte-level merging, bit-packed keys (Rank + Number + Index in ulong), parallel chunk sorters running Radix Sort.";

    private const long CHUNK  = 512L * 1024 * 1024;
    private const int  RBUF   = 4   * 1024 * 1024;
    private const int  WBUF   = 64  * 1024 * 1024;
    private const int  IBUF   = 32  * 1024 * 1024;
    private const int  PDEPTH = 1;
    private static readonly int P = Math.Max(1, Environment.ProcessorCount / 2);

    public async Task SortAsync(string inputPath, string outputPath)
    {
        string tmp = AlgoHelper.MakeTempDir(outputPath);
        try
        {
            var runs = await BuildAsync(inputPath, tmp);
            await KMergeAsync(runs, outputPath);
        }
        finally { AlgoHelper.CleanUp(tmp); }
    }

    public sealed class ByteChunk
    {
        public readonly byte[] Buffer;
        public readonly int Length;

        public ByteChunk(byte[] buffer, int length)
        {
            Buffer = buffer;
            Length = length;
        }
    }

    private async Task<List<string>> BuildAsync(string input, string tmp)
    {
        var rawCh = Channel.CreateBounded<ByteChunk>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = false });
        var srtCh = Channel.CreateBounded<(byte[] buf, int[] starts, int[] lens, ulong[] keys, int n)>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = false, SingleReader = true });
        var files = new List<string>();

        // 1. Reader Task
        var rdr = Task.Run(async () =>
        {
            using var fs = new FileStream(input, FileMode.Open, FileAccess.Read, FileShare.Read, IBUF, FileOptions.SequentialScan);
            byte[] leftover = new byte[1024 * 1024]; // 1 MB overflow area
            int leftoverLen = 0;

            while (true)
            {
                byte[] buffer = new byte[CHUNK + 1024 * 1024];
                int bytesRead = 0;

                if (leftoverLen > 0)
                {
                    Buffer.BlockCopy(leftover, 0, buffer, 0, leftoverLen);
                    bytesRead = leftoverLen;
                    leftoverLen = 0;
                }

                int targetRead = (int)CHUNK - bytesRead;
                if (targetRead > 0)
                {
                    int read = fs.Read(buffer, bytesRead, targetRead);
                    bytesRead += read;
                }

                if (bytesRead == 0) break;

                int end = bytesRead;
                while (end > 0 && buffer[end - 1] != (byte)'\n')
                {
                    end--;
                }

                if (end == 0) end = bytesRead;

                int leftoverSize = bytesRead - end;
                if (leftoverSize > 0)
                {
                    Buffer.BlockCopy(buffer, end, leftover, 0, leftoverSize);
                    leftoverLen = leftoverSize;
                }

                await rawCh.Writer.WriteAsync(new ByteChunk(buffer, end)).ConfigureAwait(false);
            }
            rawCh.Writer.Complete();
        });

        // 2. Parallel Sorter Tasks
        int sorterCount = Math.Max(1, P);
        var sorterTasks = new Task[sorterCount];
        for (int i = 0; i < sorterCount; i++)
        {
            sorterTasks[i] = Task.Run(async () =>
            {
                await foreach (var chunk in rawCh.Reader.ReadAllAsync().ConfigureAwait(false))
                {
                    var (keys, starts, lens, lineIdx) = ProcessChunk(chunk);
                    
                    var tempKeys = new ulong[lineIdx];
                    RadixSorter.RadixSort(keys, tempKeys, lineIdx);

                    await srtCh.Writer.WriteAsync((chunk.Buffer, starts, lens, keys, lineIdx)).ConfigureAwait(false);
                }
            });
        }

        var sortersDone = Task.WhenAll(sorterTasks).ContinueWith(t => srtCh.Writer.Complete());

        // 3. Writer Task
        var wrt = Task.Run(async () =>
        {
            int idx = 0;
            await foreach (var (buf, starts, lens, keys, n) in srtCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                string p = Path.Combine(tmp, $"r{idx++:D6}.lz4");
                WriteLz4(p, buf, starts, lens, keys, n);
                files.Add(p);
            }
        });

        await Task.WhenAll(rdr, Task.WhenAll(sorterTasks), sortersDone, wrt).ConfigureAwait(false);
        return files;
    }

    private static (ulong[] keys, int[] starts, int[] lens, int count) ProcessChunk(ByteChunk chunk)
    {
        int capacity = chunk.Length / 30; // Estimated line count
        int[] lineStarts = new int[capacity];
        int[] lineLengths = new int[capacity];
        int[] rawRanks = new int[capacity];
        int[] numbers = new int[capacity];

        int lineIdx = 0;
        int currStart = 0;
        var uniqueKeys = new List<byte[]>(32);

        for (int i = 0; i < chunk.Length; i++)
        {
            if (chunk.Buffer[i] == (byte)'\n')
            {
                int len = i - currStart + 1;
                if (lineIdx >= lineStarts.Length)
                {
                    int newCap = lineStarts.Length * 2;
                    Array.Resize(ref lineStarts, newCap);
                    Array.Resize(ref lineLengths, newCap);
                    Array.Resize(ref rawRanks, newCap);
                    Array.Resize(ref numbers, newCap);
                }

                lineStarts[lineIdx] = currStart;
                lineLengths[lineIdx] = len;

                var lineSpan = chunk.Buffer.AsSpan(currStart, len);
                int parseLen = len;
                while (parseLen > 0 && (lineSpan[parseLen - 1] == (byte)'\n' || lineSpan[parseLen - 1] == (byte)'\r'))
                {
                    parseLen--;
                }
                var parseSpan = lineSpan.Slice(0, parseLen);

                var (keyStart, num) = ParseKeyOffset(parseSpan);
                var keySpan = parseSpan.Slice(keyStart);

                int rawRank = -1;
                for (int j = 0; j < uniqueKeys.Count; j++)
                {
                    if (EqualsIgnoreCase(keySpan, uniqueKeys[j]))
                    {
                        rawRank = j;
                        break;
                    }
                }
                if (rawRank == -1)
                {
                    byte[] newKey = keySpan.ToArray();
                    rawRank = uniqueKeys.Count;
                    uniqueKeys.Add(newKey);
                }

                rawRanks[lineIdx] = rawRank;
                numbers[lineIdx] = num;

                lineIdx++;
                currStart = i + 1;
            }
        }

        if (currStart < chunk.Length)
        {
            int len = chunk.Length - currStart;
            if (lineIdx >= lineStarts.Length)
            {
                int newCap = lineStarts.Length * 2;
                Array.Resize(ref lineStarts, newCap);
                Array.Resize(ref lineLengths, newCap);
                Array.Resize(ref rawRanks, newCap);
                Array.Resize(ref numbers, newCap);
            }

            lineStarts[lineIdx] = currStart;
            lineLengths[lineIdx] = len;

            var lineSpan = chunk.Buffer.AsSpan(currStart, len);
            int parseLen = len;
            while (parseLen > 0 && (lineSpan[parseLen - 1] == (byte)'\n' || lineSpan[parseLen - 1] == (byte)'\r'))
            {
                parseLen--;
            }
            var parseSpan = lineSpan.Slice(0, parseLen);

            var (keyStart, num) = ParseKeyOffset(parseSpan);
            var keySpan = parseSpan.Slice(keyStart);

            int rawRank = -1;
            for (int j = 0; j < uniqueKeys.Count; j++)
            {
                if (EqualsIgnoreCase(keySpan, uniqueKeys[j]))
                {
                    rawRank = j;
                    break;
                }
            }
            if (rawRank == -1)
            {
                byte[] newKey = keySpan.ToArray();
                rawRank = uniqueKeys.Count;
                uniqueKeys.Add(newKey);
            }

            rawRanks[lineIdx] = rawRank;
            numbers[lineIdx] = num;
            lineIdx++;
        }

        // Map raw distinct ranks to alphabetical rank indices
        var uniqueStrings = new List<string>(uniqueKeys.Count);
        var rawUniqueStrings = new List<string>(uniqueKeys.Count);
        for (int i = 0; i < uniqueKeys.Count; i++)
        {
            string s = Encoding.UTF8.GetString(uniqueKeys[i]);
            uniqueStrings.Add(s);
            rawUniqueStrings.Add(s);
        }
        uniqueStrings.Sort(StringComparer.OrdinalIgnoreCase);

        int[] rankMapping = new int[uniqueKeys.Count];
        for (int i = 0; i < uniqueKeys.Count; i++)
        {
            rankMapping[i] = uniqueStrings.IndexOf(rawUniqueStrings[i]);
        }

        ulong[] keys = new ulong[lineIdx];

        Parallel.ForEach(System.Collections.Concurrent.Partitioner.Create(0, lineIdx), range =>
        {
            for (int i = range.Item1; i < range.Item2; i++)
            {
                int rank = rankMapping[rawRanks[i]];
                keys[i] = ((ulong)rank << 58) | ((ulong)numbers[i] << 27) | (uint)i;
            }
        });

        return (keys, lineStarts, lineLengths, lineIdx);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (int keyStart, int num) ParseKeyOffset(ReadOnlySpan<byte> line)
    {
        int num = 0;
        int len = line.Length;
        for (int i = 0; i < len; i++)
        {
            byte b = line[i];
            if (b == (byte)'.')
            {
                int ts = i + 1;
                if (ts < len && line[ts] == (byte)' ') ts++;
                return (ts, num);
            }
            uint d = (uint)(b - (byte)'0');
            if (d <= 9) num = num * 10 + (int)d;
        }
        return (0, 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EqualsIgnoreCase(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
        {
            byte c1 = a[i];
            byte c2 = b[i];
            if (c1 == c2) continue;
            if (c1 >= 65 && c1 <= 90) c1 += 32;
            if (c2 >= 65 && c2 <= 90) c2 += 32;
            if (c1 != c2) return false;
        }
        return true;
    }

    private static void WriteLz4(string path, byte[] buffer, int[] starts, int[] lens, ulong[] keys, int count)
    {
        using var fs  = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        using var lz4 = LZ4Stream.Encode(fs, K4os.Compression.LZ4.LZ4Level.L00_FAST, leaveOpen: true);
        
        byte[] writeBuffer = new byte[64 * 1024 * 1024];
        int writeOffset = 0;

        for (int i = 0; i < count; i++)
        {
            int idx = (int)(keys[i] & 0x07FFFFFF);
            int start = starts[idx];
            int len = lens[idx];

            if (writeOffset + len > writeBuffer.Length)
            {
                lz4.Write(writeBuffer, 0, writeOffset);
                writeOffset = 0;
            }

            Buffer.BlockCopy(buffer, start, writeBuffer, writeOffset, len);
            writeOffset += len;
        }

        if (writeOffset > 0)
        {
            lz4.Write(writeBuffer, 0, writeOffset);
        }
    }

    private Task KMergeAsync(List<string> paths, string output)
    {
        return Task.Run(() =>
        {
            int k = paths.Count;
            var rdrs = new ByteRunReader[k];
            try
            {
                for (int i = 0; i < k; i++) rdrs[i] = new ByteRunReader(paths[i], 1024 * 1024);
                
                var heap = new PriorityQueue<ByteMergeEntry, ByteMergeEntry>(k, ByteMergeEntryComparer.Instance);
                for (int i = 0; i < k; i++)
                {
                    if (rdrs[i].TryReadLine(out int lineStart, out int lineLength))
                    {
                        var lineSpan = rdrs[i].Buffer.AsSpan(lineStart, lineLength);
                        
                        int parseLen = lineLength;
                        while (parseLen > 0 && (lineSpan[parseLen - 1] == (byte)'\n' || lineSpan[parseLen - 1] == (byte)'\r'))
                        {
                            parseLen--;
                        }
                        var parseSpan = lineSpan.Slice(0, parseLen);
                        
                        var (keyStart, keyLen, num) = ParseKeyOffsetForMerge(parseSpan);
                        var entry = new ByteMergeEntry(rdrs[i].Buffer, lineStart + keyStart, keyLen, num, lineStart, lineLength, i);
                        heap.Enqueue(entry, entry);
                    }
                }

                using var ofs = new FileStream(output, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
                while (heap.Count > 0)
                {
                    var item = heap.Dequeue();
                    
                    ofs.Write(item.Buffer, item.LineStart, item.LineLength);

                    if (rdrs[item.ReaderIndex].TryReadLine(out int lineStart, out int lineLength))
                    {
                        var lineSpan = rdrs[item.ReaderIndex].Buffer.AsSpan(lineStart, lineLength);
                        
                        int parseLen = lineLength;
                        while (parseLen > 0 && (lineSpan[parseLen - 1] == (byte)'\n' || lineSpan[parseLen - 1] == (byte)'\r'))
                        {
                            parseLen--;
                        }
                        var parseSpan = lineSpan.Slice(0, parseLen);
                        
                        var (keyStart, keyLen, num) = ParseKeyOffsetForMerge(parseSpan);
                        var entry = new ByteMergeEntry(rdrs[item.ReaderIndex].Buffer, lineStart + keyStart, keyLen, num, lineStart, lineLength, item.ReaderIndex);
                        heap.Enqueue(entry, entry);
                    }
                }
            }
            finally
            {
                for (int i = 0; i < k; i++)
                {
                    if (rdrs[i] != null)
                    {
                        rdrs[i].Dispose();
                        try { File.Delete(paths[i]); } catch { }
                    }
                }
            }
        });
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (int keyStart, int keyLen, int num) ParseKeyOffsetForMerge(ReadOnlySpan<byte> line)
    {
        int num = 0;
        int len = line.Length;
        for (int i = 0; i < len; i++)
        {
            byte b = line[i];
            if (b == (byte)'.')
            {
                int ts = i + 1;
                if (ts < len && line[ts] == (byte)' ') ts++;
                return (ts, len - ts, num);
            }
            uint d = (uint)(b - (byte)'0');
            if (d <= 9) num = num * 10 + (int)d;
        }
        return (0, 0, 0);
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
//  V7 ZERO-ALLOCATION POOL-PIPELINE + RADIX + BYTE-LEVEL MERGE
// ═══════════════════════════════════════════════════════════════════════════════

public sealed class PooledByteRunReader : IDisposable
{
    private readonly Stream _stream;
    private readonly byte[] _buffer;
    private int _bufferStart;
    private int _bufferEnd;
    private bool _eof;

    public byte[] Buffer => _buffer;

    public PooledByteRunReader(string path, int bufferSize)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, FileOptions.SequentialScan);
        _stream = LZ4Stream.Decode(fs, leaveOpen: false);
        _buffer = ZeroAllocPoolRadixByteSortAlgorithm.BytePool.Rent(bufferSize);
        _bufferStart = 0;
        _bufferEnd = 0;
        _eof = false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryReadLine(out int lineStart, out int lineLength)
    {
        lineStart = 0;
        lineLength = 0;

        while (true)
        {
            // Scan for newline in the current buffer
            int searchStart = _bufferStart;
            int newlineIdx = -1;
            for (int i = searchStart; i < _bufferEnd; i++)
            {
                if (_buffer[i] == (byte)'\n')
                {
                    newlineIdx = i;
                    break;
                }
            }

            if (newlineIdx != -1)
            {
                lineStart = _bufferStart;
                lineLength = newlineIdx - _bufferStart + 1;
                _bufferStart = newlineIdx + 1;
                return true;
            }

            // No newline found. If EOF, return whatever is left (if anything)
            if (_eof)
            {
                if (_bufferStart < _bufferEnd)
                {
                    lineStart = _bufferStart;
                    lineLength = _bufferEnd - _bufferStart;
                    _bufferStart = _bufferEnd;
                    return true;
                }
                return false;
            }

            // Compact buffer and read more
            int remaining = _bufferEnd - _bufferStart;
            if (remaining > 0)
            {
                System.Buffer.BlockCopy(_buffer, _bufferStart, _buffer, 0, remaining);
            }
            _bufferStart = 0;
            _bufferEnd = remaining;

            int read = _stream.Read(_buffer, _bufferEnd, _buffer.Length - _bufferEnd);
            if (read == 0)
            {
                _eof = true;
            }
            else
            {
                _bufferEnd += read;
            }
        }
    }

    public void Dispose()
    {
        _stream.Dispose();
        ZeroAllocPoolRadixByteSortAlgorithm.BytePool.Return(_buffer);
    }
}

public sealed class ZeroAllocPoolRadixByteSortAlgorithm : ISortAlgorithm
{
    public string Name        => "V7 Zero-Allocation Pool-Pipeline + Radix + Byte-Level Merge";
    public string Description => "V7 with complete ArrayPool buffer recycling, dynamically calculated chunk sizes to saturate CPU cores, and direct byte writes.";

    // Custom pools optimized for large arrays to avoid heap allocations on System.Buffers.ArrayPool.Shared (which is capped at 1M elements)
    public static readonly System.Buffers.ArrayPool<byte> BytePool = System.Buffers.ArrayPool<byte>.Create(512 * 1024 * 1024, 16);
    public static readonly System.Buffers.ArrayPool<int> IntPool = System.Buffers.ArrayPool<int>.Create(64 * 1024 * 1024, 32);
    public static readonly System.Buffers.ArrayPool<ulong> UlongPool = System.Buffers.ArrayPool<ulong>.Create(64 * 1024 * 1024, 16);

    private const int  RBUF   = 4   * 1024 * 1024;
    private const int  WBUF   = 64  * 1024 * 1024;
    private const int  IBUF   = 32  * 1024 * 1024;
    private const int  PDEPTH = 2; // Bounded channel queue capacity

    public async Task SortAsync(string inputPath, string outputPath)
    {
        string tmp = AlgoHelper.MakeTempDir(outputPath);
        try
        {
            var runs = await BuildAsync(inputPath, tmp);
            await KMergeAsync(runs, outputPath);
        }
        finally { AlgoHelper.CleanUp(tmp); }
    }

    public sealed class ByteChunk
    {
        public readonly byte[] Buffer;
        public readonly int Length;

        public ByteChunk(byte[] buffer, int length)
        {
            Buffer = buffer;
            Length = length;
        }
    }

    private async Task<List<string>> BuildAsync(string input, string tmp)
    {
        // 0. Compute optimal chunk size dynamically (target 4 chunks to balance parallelism and merging)
        long totalSize = new FileInfo(input).Length;
        int cores = Environment.ProcessorCount;
        int activeTasks = 4;
        long chunkSize = totalSize / Math.Max(1, activeTasks);
        // Clamp chunk size between 64MB and 512MB to balance memory use and concurrency
        chunkSize = Math.Clamp(chunkSize, 64L * 1024 * 1024, 512L * 1024 * 1024);

        var rawCh = Channel.CreateBounded<ByteChunk>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true, SingleReader = false });
        var srtCh = Channel.CreateBounded<(byte[] buf, int[] starts, int[] lens, ulong[] keys, int n)>(new BoundedChannelOptions(PDEPTH) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = false, SingleReader = true });
        var files = new List<string>();

        // 1. Reader Task
        var rdr = Task.Run(async () =>
        {
            using var fs = new FileStream(input, FileMode.Open, FileAccess.Read, FileShare.Read, IBUF, FileOptions.SequentialScan);
            byte[] leftover = new byte[1024 * 1024]; // 1 MB overflow area
            int leftoverLen = 0;

            while (true)
            {
                // Rent chunk buffer from custom pool
                byte[] buffer = BytePool.Rent((int)chunkSize + 1024 * 1024);
                int bytesRead = 0;

                if (leftoverLen > 0)
                {
                    Buffer.BlockCopy(leftover, 0, buffer, 0, leftoverLen);
                    bytesRead = leftoverLen;
                    leftoverLen = 0;
                }

                int targetRead = (int)chunkSize - bytesRead;
                if (targetRead > 0)
                {
                    int read = fs.Read(buffer, bytesRead, targetRead);
                    bytesRead += read;
                }

                if (bytesRead == 0)
                {
                    BytePool.Return(buffer);
                    break;
                }

                int end = bytesRead;
                while (end > 0 && buffer[end - 1] != (byte)'\n')
                {
                    end--;
                }

                if (end == 0) end = bytesRead;

                int leftoverSize = bytesRead - end;
                if (leftoverSize > 0)
                {
                    Buffer.BlockCopy(buffer, end, leftover, 0, leftoverSize);
                    leftoverLen = leftoverSize;
                }

                await rawCh.Writer.WriteAsync(new ByteChunk(buffer, end)).ConfigureAwait(false);
            }
            rawCh.Writer.Complete();
        });

        // 2. Parallel Sorter Tasks (one per active task)
        int sorterCount = activeTasks;
        var sorterTasks = new Task[sorterCount];
        for (int i = 0; i < sorterCount; i++)
        {
            sorterTasks[i] = Task.Run(async () =>
            {
                await foreach (var chunk in rawCh.Reader.ReadAllAsync().ConfigureAwait(false))
                {
                    var (keys, starts, lens, lineIdx) = ProcessChunk(chunk);
                    
                    // Rent temporary keys array for Radix Sort from UlongPool
                    var tempKeys = UlongPool.Rent(lineIdx);
                    RadixSorter.RadixSort(keys, tempKeys, lineIdx);
                    UlongPool.Return(tempKeys);

                    await srtCh.Writer.WriteAsync((chunk.Buffer, starts, lens, keys, lineIdx)).ConfigureAwait(false);
                }
            });
        }

        var sortersDone = Task.WhenAll(sorterTasks).ContinueWith(t => srtCh.Writer.Complete());

        // 3. Writer Task (returns all rented arrays back to custom pools)
        var wrt = Task.Run(async () =>
        {
            int idx = 0;
            await foreach (var (buf, starts, lens, keys, n) in srtCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                string p = Path.Combine(tmp, $"r{idx++:D6}.lz4");
                WriteLz4(p, buf, starts, lens, keys, n);
                files.Add(p);

                // Return rented arrays to the custom pools
                BytePool.Return(buf);
                IntPool.Return(starts);
                IntPool.Return(lens);
                UlongPool.Return(keys);
            }
        });

        await Task.WhenAll(rdr, Task.WhenAll(sorterTasks), sortersDone, wrt).ConfigureAwait(false);
        return files;
    }

    private static (ulong[] keys, int[] starts, int[] lens, int count) ProcessChunk(ByteChunk chunk)
    {
        // Safe under-estimate (average 16 bytes per line + safety margin) to avoid any resizes
        int capacity = chunk.Length / 16 + 1000;
        
        // Rent arrays from the custom IntPool
        int[] lineStarts = IntPool.Rent(capacity);
        int[] lineLengths = IntPool.Rent(capacity);
        int[] rawRanks = IntPool.Rent(capacity);
        int[] numbers = IntPool.Rent(capacity);

        int lineIdx = 0;
        int currStart = 0;
        var uniqueKeys = new List<byte[]>(32);

        for (int i = 0; i < chunk.Length; i++)
        {
            if (chunk.Buffer[i] == (byte)'\n')
            {
                int len = i - currStart + 1;
                if (lineIdx >= lineStarts.Length)
                {
                    int newCap = lineStarts.Length * 2;
                    
                    int[] newLineStarts = IntPool.Rent(newCap);
                    Buffer.BlockCopy(lineStarts, 0, newLineStarts, 0, lineIdx * sizeof(int));
                    IntPool.Return(lineStarts);
                    lineStarts = newLineStarts;

                    int[] newLineLengths = IntPool.Rent(newCap);
                    Buffer.BlockCopy(lineLengths, 0, newLineLengths, 0, lineIdx * sizeof(int));
                    IntPool.Return(lineLengths);
                    lineLengths = newLineLengths;

                    int[] newRawRanks = IntPool.Rent(newCap);
                    Buffer.BlockCopy(rawRanks, 0, newRawRanks, 0, lineIdx * sizeof(int));
                    IntPool.Return(rawRanks);
                    rawRanks = newRawRanks;

                    int[] newNumbers = IntPool.Rent(newCap);
                    Buffer.BlockCopy(numbers, 0, newNumbers, 0, lineIdx * sizeof(int));
                    IntPool.Return(numbers);
                    numbers = newNumbers;
                }

                lineStarts[lineIdx] = currStart;
                lineLengths[lineIdx] = len;

                var lineSpan = chunk.Buffer.AsSpan(currStart, len);
                int parseLen = len;
                while (parseLen > 0 && (lineSpan[parseLen - 1] == (byte)'\n' || lineSpan[parseLen - 1] == (byte)'\r'))
                {
                    parseLen--;
                }
                var parseSpan = lineSpan.Slice(0, parseLen);

                var (keyStart, num) = ParseKeyOffset(parseSpan);
                var keySpan = parseSpan.Slice(keyStart);

                int rawRank = -1;
                for (int j = 0; j < uniqueKeys.Count; j++)
                {
                    if (EqualsIgnoreCase(keySpan, uniqueKeys[j]))
                    {
                        rawRank = j;
                        break;
                    }
                }
                if (rawRank == -1)
                {
                    byte[] newKey = keySpan.ToArray();
                    rawRank = uniqueKeys.Count;
                    uniqueKeys.Add(newKey);
                }

                rawRanks[lineIdx] = rawRank;
                numbers[lineIdx] = num;

                lineIdx++;
                currStart = i + 1;
            }
        }

        if (currStart < chunk.Length)
        {
            int len = chunk.Length - currStart;
            if (lineIdx >= lineStarts.Length)
            {
                int newCap = lineStarts.Length * 2;
                
                int[] newLineStarts = IntPool.Rent(newCap);
                Buffer.BlockCopy(lineStarts, 0, newLineStarts, 0, lineIdx * sizeof(int));
                IntPool.Return(lineStarts);
                lineStarts = newLineStarts;

                int[] newLineLengths = IntPool.Rent(newCap);
                Buffer.BlockCopy(lineLengths, 0, newLineLengths, 0, lineIdx * sizeof(int));
                IntPool.Return(lineLengths);
                lineLengths = newLineLengths;

                int[] newRawRanks = IntPool.Rent(newCap);
                Buffer.BlockCopy(rawRanks, 0, newRawRanks, 0, lineIdx * sizeof(int));
                IntPool.Return(rawRanks);
                rawRanks = newRawRanks;

                int[] newNumbers = IntPool.Rent(newCap);
                Buffer.BlockCopy(numbers, 0, newNumbers, 0, lineIdx * sizeof(int));
                IntPool.Return(numbers);
                numbers = newNumbers;
            }

            lineStarts[lineIdx] = currStart;
            lineLengths[lineIdx] = len;

            var lineSpan = chunk.Buffer.AsSpan(currStart, len);
            int parseLen = len;
            while (parseLen > 0 && (lineSpan[parseLen - 1] == (byte)'\n' || lineSpan[parseLen - 1] == (byte)'\r'))
            {
                parseLen--;
            }
            var parseSpan = lineSpan.Slice(0, parseLen);

            var (keyStart, num) = ParseKeyOffset(parseSpan);
            var keySpan = parseSpan.Slice(keyStart);

            int rawRank = -1;
            for (int j = 0; j < uniqueKeys.Count; j++)
            {
                if (EqualsIgnoreCase(keySpan, uniqueKeys[j]))
                {
                    rawRank = j;
                    break;
                }
            }
            if (rawRank == -1)
            {
                byte[] newKey = keySpan.ToArray();
                rawRank = uniqueKeys.Count;
                uniqueKeys.Add(newKey);
            }

            rawRanks[lineIdx] = rawRank;
            numbers[lineIdx] = num;
            lineIdx++;
        }

        // Map raw distinct ranks to alphabetical rank indices
        var uniqueStrings = new List<string>(uniqueKeys.Count);
        var rawUniqueStrings = new List<string>(uniqueKeys.Count);
        for (int i = 0; i < uniqueKeys.Count; i++)
        {
            string s = Encoding.UTF8.GetString(uniqueKeys[i]);
            uniqueStrings.Add(s);
            rawUniqueStrings.Add(s);
        }
        uniqueStrings.Sort(StringComparer.OrdinalIgnoreCase);

        int[] rankMapping = new int[uniqueKeys.Count];
        for (int i = 0; i < uniqueKeys.Count; i++)
        {
            rankMapping[i] = uniqueStrings.IndexOf(rawUniqueStrings[i]);
        }

        // Rent final keys array from UlongPool
        ulong[] keys = UlongPool.Rent(lineIdx);

        Parallel.ForEach(System.Collections.Concurrent.Partitioner.Create(0, lineIdx), range =>
        {
            for (int i = range.Item1; i < range.Item2; i++)
            {
                int rank = rankMapping[rawRanks[i]];
                keys[i] = ((ulong)rank << 58) | ((ulong)numbers[i] << 27) | (uint)i;
            }
        });

        // Return temporary parsing arrays to the custom IntPool
        IntPool.Return(rawRanks);
        IntPool.Return(numbers);

        return (keys, lineStarts, lineLengths, lineIdx);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (int keyStart, int num) ParseKeyOffset(ReadOnlySpan<byte> line)
    {
        int num = 0;
        int len = line.Length;
        for (int i = 0; i < len; i++)
        {
            byte b = line[i];
            if (b == (byte)'.')
            {
                int ts = i + 1;
                if (ts < len && line[ts] == (byte)' ') ts++;
                return (ts, num);
            }
            uint d = (uint)(b - (byte)'0');
            if (d <= 9) num = num * 10 + (int)d;
        }
        return (0, 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EqualsIgnoreCase(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != b.Length) return false;
        for (int i = 0; i < a.Length; i++)
        {
            byte c1 = a[i];
            byte c2 = b[i];
            if (c1 == c2) continue;
            if (c1 >= 65 && c1 <= 90) c1 += 32;
            if (c2 >= 65 && c2 <= 90) c2 += 32;
            if (c1 != c2) return false;
        }
        return true;
    }

    private static void WriteLz4(string path, byte[] buffer, int[] starts, int[] lens, ulong[] keys, int count)
    {
        using var fs  = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
        using var lz4 = LZ4Stream.Encode(fs, K4os.Compression.LZ4.LZ4Level.L00_FAST, leaveOpen: true);
        
        // Rent a write buffer to minimize method call overhead from custom BytePool
        byte[] writeBuffer = BytePool.Rent(4 * 1024 * 1024);
        int writeOffset = 0;

        try
        {
            for (int i = 0; i < count; i++)
            {
                int idx = (int)(keys[i] & 0x07FFFFFF);
                int start = starts[idx];
                int len = lens[idx];

                if (writeOffset + len > writeBuffer.Length)
                {
                    lz4.Write(writeBuffer, 0, writeOffset);
                    writeOffset = 0;
                }

                Buffer.BlockCopy(buffer, start, writeBuffer, writeOffset, len);
                writeOffset += len;
            }

            if (writeOffset > 0)
            {
                lz4.Write(writeBuffer, 0, writeOffset);
            }
        }
        finally
        {
            BytePool.Return(writeBuffer);
        }
    }

    private Task KMergeAsync(List<string> paths, string output)
    {
        return Task.Run(() =>
        {
            int k = paths.Count;
            var rdrs = new PooledByteRunReader[k];
            try
            {
                for (int i = 0; i < k; i++) rdrs[i] = new PooledByteRunReader(paths[i], 1024 * 1024);
                
                var heap = new PriorityQueue<ByteMergeEntry, ByteMergeEntry>(k, ByteMergeEntryComparer.Instance);
                for (int i = 0; i < k; i++)
                {
                    if (rdrs[i].TryReadLine(out int lineStart, out int lineLength))
                    {
                        var lineSpan = rdrs[i].Buffer.AsSpan(lineStart, lineLength);
                        
                        int parseLen = lineLength;
                        while (parseLen > 0 && (lineSpan[parseLen - 1] == (byte)'\n' || lineSpan[parseLen - 1] == (byte)'\r'))
                        {
                            parseLen--;
                        }
                        var parseSpan = lineSpan.Slice(0, parseLen);
                        
                        var (keyStart, keyLen, num) = ParseKeyOffsetForMerge(parseSpan);
                        var entry = new ByteMergeEntry(rdrs[i].Buffer, lineStart + keyStart, keyLen, num, lineStart, lineLength, i);
                        heap.Enqueue(entry, entry);
                    }
                }

                using var ofs = new FileStream(output, FileMode.Create, FileAccess.Write, FileShare.None, WBUF, FileOptions.SequentialScan);
                while (heap.Count > 0)
                {
                    var item = heap.Dequeue();
                    
                    ofs.Write(item.Buffer, item.LineStart, item.LineLength);

                    if (rdrs[item.ReaderIndex].TryReadLine(out int lineStart, out int lineLength))
                    {
                        var lineSpan = rdrs[item.ReaderIndex].Buffer.AsSpan(lineStart, lineLength);
                        
                        int parseLen = lineLength;
                        while (parseLen > 0 && (lineSpan[parseLen - 1] == (byte)'\n' || lineSpan[parseLen - 1] == (byte)'\r'))
                        {
                            parseLen--;
                        }
                        var parseSpan = lineSpan.Slice(0, parseLen);
                        
                        var (keyStart, keyLen, num) = ParseKeyOffsetForMerge(parseSpan);
                        var entry = new ByteMergeEntry(rdrs[item.ReaderIndex].Buffer, lineStart + keyStart, keyLen, num, lineStart, lineLength, item.ReaderIndex);
                        heap.Enqueue(entry, entry);
                    }
                }
            }
            finally
            {
                for (int i = 0; i < k; i++)
                {
                    if (rdrs[i] != null)
                    {
                        rdrs[i].Dispose();
                        try { File.Delete(paths[i]); } catch { }
                    }
                }
            }
        });
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (int keyStart, int keyLen, int num) ParseKeyOffsetForMerge(ReadOnlySpan<byte> line)
    {
        int num = 0;
        int len = line.Length;
        for (int i = 0; i < len; i++)
        {
            byte b = line[i];
            if (b == (byte)'.')
            {
                int ts = i + 1;
                if (ts < len && line[ts] == (byte)' ') ts++;
                return (ts, len - ts, num);
            }
            uint d = (uint)(b - (byte)'0');
            if (d <= 9) num = num * 10 + (int)d;
        }
        return (0, 0, 0);
    }
}


