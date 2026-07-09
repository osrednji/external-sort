using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using K4os.Compression.LZ4.Streams;

// ═══════════════════════════════════════════════════════════════════════════════
//  — 100 GB-CAPABLE RADIX SORT + MULTI-PASS BYTE-LEVEL MERGE
//
//  Fixes over original V9:
//  1. Bit-packing overflow: rank+number each get 32 bits; index in separate int[].
//  2. O(N²) rankMapping IndexOf → Dictionary<string,int>.
//  3. O(N²) unique-key linear scan → FNV-1a open-addressing hash map that
//     doubles at load 0.5 (no infinite-loop possible).
//  4. RadixSortWithIndices: ReferenceEquals guard ensures result is always
//     in the caller's arrays regardless of pass count.
//  5. WriteLz4 no longer has unused `keys` parameter.
//  6. keys[] returned to pool immediately after sort; not carried in channel.
//  7. Pool-return safety: all rented arrays returned in finally blocks.
//  8. [100 GB FIX] Single-pass K-way merge replaced with multi-pass fanout
//     merge (FAN=16). A 100 GB file produces ~3300 run files; opening them all
//     at once would exceed OS file-handle limits (~1024) and consume 3+ GB RAM
//     in read buffers alone. Multi-pass keeps at most FAN files open at once.
//  9. [100 GB FIX] counts[] inside RadixSort rented from IntPool instead of
//     heap-allocated (256 KB × 3300 chunks = 858 MB of GC pressure avoided).
// ═══════════════════════════════════════════════════════════════════════════════

namespace Benchmark;

/// <summary>
/// A high-performance external sorting algorithm capable of sorting files larger than RAM (up to 100 GB+).
/// Features a core-saturated parallel radix sorting step and an LZ4-buffered multi-pass K-way merge step.
/// </summary>
public sealed class SortingAlgorithm : ISortAlgorithm
{
    /// <summary>Gets the display name of this sorting algorithm.</summary>
    public string Name => "SortingAlgorithm";

    /// <summary>Gets a detailed description of the sorting pipeline optimizations.</summary>
    public string Description =>
        "100 GB-capable: multi-pass fanout merge (FAN=16), core-saturated " +
        "dynamic chunk size, zero-allocation ArrayPool, parallel Radix Sort, " +
        "separate index array, O(1) resizing hash-map, O(N) rank mapping, " +
        "byte-level merge.";

    // ── Merge fan-in: at most this many files open simultaneously ───────────
    // With FAN=16 and ~3300 runs for 100 GB:
    //   pass 1: 3300 → 207 intermediate runs
    //   pass 2: 207  → 13  intermediate runs
    //   pass 3: 13   → final output
    private const int Fan = 16;

    // ── I/O buffer sizes ────────────────────────────────────────────────────
    private const int Wbuf = 64 * 1024 * 1024;
    private const int Ibuf = 32 * 1024 * 1024;
    private const int Pdepth = 1;

    // ── ArrayPools ──────────────────────────────────────────────────────────
    private static System.Buffers.ArrayPool<byte> BytePool => BenchmarkPools.BytePool;
    private static System.Buffers.ArrayPool<int> IntPool => BenchmarkPools.IntPool;
    private static System.Buffers.ArrayPool<ulong> UlongPool => BenchmarkPools.UlongPool;

    // ── Pipeline message ────────────────────────────────────────────────────
    // Ownership: sorter rents all arrays, passes via channel, writer returns them.
    private readonly struct SortedChunk
    {
        public readonly byte[] Buffer;   // rented BytePool  — raw chunk bytes
        public readonly int[] Starts;   // rented IntPool   — line byte offsets
        public readonly int[] Lens;     // rented IntPool   — line byte lengths
        public readonly int[] Indices;  // rented IntPool   — sort permutation
        public readonly int Count;

        public SortedChunk(byte[] buf, int[] starts, int[] lens,
            int[] indices, int count)
        {
            Buffer = buf;
            Starts = starts;
            Lens = lens;
            Indices = indices;
            Count = count;
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Public entry point
    // ═══════════════════════════════════════════════════════════════════════
    
    /// <summary>
    /// Executes the sorting benchmark flow asynchronously on the specified file.
    /// </summary>
    /// <param name="inputPath">The absolute path to the unsorted source text file.</param>
    /// <param name="outputPath">The absolute path where the sorted text file should be created.</param>
    /// <returns>A task representing the asynchronous sorting process.</returns>
    public async Task SortAsync(string inputPath, string outputPath)
    {
        var tmp = AlgoHelper.MakeTempDir(outputPath);
        try
        {
            var runs = await BuildAsync(inputPath, tmp).ConfigureAwait(false);
            await MultiPassMergeAsync(runs, tmp, outputPath).ConfigureAwait(false);
        }
        finally { AlgoHelper.CleanUp(tmp); }
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Phase 1 — Build sorted LZ4 run files
    // ═══════════════════════════════════════════════════════════════════════
    private async Task<List<string>> BuildAsync(string input, string tmp)
    {
        var totalSize = new FileInfo(input).Length;
        var cores = Environment.ProcessorCount;
        var workers = Math.Min(6, cores);
        var chunkSize = Math.Clamp(totalSize / Math.Max(1, workers),
            15L * 1024 * 1024,
            31L * 1024 * 1024);

        var rawCh = Channel.CreateBounded<(byte[] buf, int len)>(
            new BoundedChannelOptions(Pdepth)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true,
                SingleReader = false
            });

        var srtCh = Channel.CreateBounded<SortedChunk>(
            new BoundedChannelOptions(Pdepth)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = false,
                SingleReader = true
            });

        var files = new List<string>();

        // ── 1. Reader ──────────────────────────────────────────────────────
        var rdr = Task.Run(async () =>
        {
            using var fs = new FileStream(input, FileMode.Open, FileAccess.Read,
                FileShare.Read, Ibuf,
                FileOptions.SequentialScan);
            var leftover = new byte[1024 * 1024];
            var leftoverLen = 0;

            while (true)
            {
                var buffer = BytePool.Rent((int)chunkSize + 1024 * 1024);
                var bytesRead = 0;

                if (leftoverLen > 0)
                {
                    Buffer.BlockCopy(leftover, 0, buffer, 0, leftoverLen);
                    bytesRead = leftoverLen;
                    leftoverLen = 0;
                }

                var targetRead = (int)chunkSize - bytesRead;
                if (targetRead > 0)
                {
                    var read = fs.Read(buffer, bytesRead, targetRead);
                    bytesRead += read;
                }

                if (bytesRead == 0) { BytePool.Return(buffer); break; }

                // Trim to last complete line
                var end = bytesRead;
                while (end > 0 && buffer[end - 1] != (byte)'\n') end--;
                if (end == 0) end = bytesRead;

                var spill = bytesRead - end;
                if (spill > 0)
                {
                    Buffer.BlockCopy(buffer, end, leftover, 0, spill);
                    leftoverLen = spill;
                }

                await rawCh.Writer.WriteAsync((buffer, end)).ConfigureAwait(false);
            }
            rawCh.Writer.Complete();
        });

        // ── 2. Parallel sorters ────────────────────────────────────────────
        var sorterTasks = new Task[workers];
        for (var i = 0; i < workers; i++)
        {
            sorterTasks[i] = Task.Run(async () =>
            {
                await foreach (var (buf, len) in
                               rawCh.Reader.ReadAllAsync().ConfigureAwait(false))
                {
                    var sc = ProcessAndSort(buf, len);
                    await srtCh.Writer.WriteAsync(sc).ConfigureAwait(false);
                }
            });
        }

        // ── 3. Writer ──────────────────────────────────────────────────────
        var wrt = Task.Run(async () =>
        {
            var idx = 0;
            await foreach (var sc in
                           srtCh.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                var p = Path.Combine(tmp, $"r{idx++:D6}.lz4");
                try
                {
                    WriteLz4(p, sc.Buffer, sc.Starts, sc.Lens,
                        sc.Indices, sc.Count);
                    files.Add(p);
                }
                finally
                {
                    // Fix #7: returned even if WriteLz4 throws
                    BytePool.Return(sc.Buffer);
                    IntPool.Return(sc.Starts);
                    IntPool.Return(sc.Lens);
                    IntPool.Return(sc.Indices);
                }
            }
        });

        await rdr.ConfigureAwait(false);
        await Task.WhenAll(sorterTasks).ConfigureAwait(false);
        srtCh.Writer.Complete();
        await wrt.ConfigureAwait(false);

        return files;
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Phase 2 — Multi-pass fanout merge
    //
    //  This implementation keeps at most FAN=16 files open per merge batch.
    //  Intermediate passes write LZ4-compressed files to save disk space.
    //  The final pass writes plain text directly to outputPath.
    // ═══════════════════════════════════════════════════════════════════════
    private async Task MultiPassMergeAsync(
        List<string> runs, string tmp, string output)
    {
        var passNum = 0;

        // Reduce until we have FAN or fewer runs
        while (runs.Count > Fan)
        {
            var nextRuns = new List<string>((runs.Count + Fan - 1) / Fan);

            for (var i = 0; i < runs.Count; i += Fan)
            {
                var batchSize = Math.Min(Fan, runs.Count - i);
                var batch = runs.GetRange(i, batchSize);
                var dest = Path.Combine(
                    tmp, $"p{passNum:D4}_{i:D6}.lz4");

                // Run synchronously on thread pool — keeps disk I/O sequential
                await Task.Run(() => MergeBatchToLz4(batch, dest))
                    .ConfigureAwait(false);

                nextRuns.Add(dest);
            }

            runs = nextRuns;
            passNum++;
        }

        // Final pass: merge remaining runs → plain-text output
        await Task.Run(() => MergeBatchToPlainText(runs, output))
            .ConfigureAwait(false);
    }

    // Merges a batch of LZ4 run files into a single LZ4 intermediate file.
    private static void MergeBatchToLz4(List<string> paths, string output)
    {
        var k = paths.Count;
        var rdrs = new PooledByteRunReader[k];
        try
        {
            for (var i = 0; i < k; i++)
                rdrs[i] = new PooledByteRunReader(paths[i], 1024 * 1024);

            var heap = BuildHeap(rdrs, k);

            using var fs = new FileStream(output, FileMode.Create,
                FileAccess.Write, FileShare.None,
                Wbuf, FileOptions.SequentialScan);
            using var lz4 = LZ4Stream.Encode(fs,
                K4os.Compression.LZ4.LZ4Level.L00_FAST,
                leaveOpen: true);

            var wb = BytePool.Rent(4 * 1024 * 1024);
            var wOff = 0;
            try
            {
                DrainHeap(heap, rdrs, (buf, start, len) =>
                {
                    if (wOff + len > wb.Length)
                    { lz4.Write(wb, 0, wOff); wOff = 0; }
                    Buffer.BlockCopy(buf, start, wb, wOff, len);
                    wOff += len;
                });
                if (wOff > 0) lz4.Write(wb, 0, wOff);
            }
            finally { BytePool.Return(wb); }
        }
        finally
        {
            for (var i = 0; i < k; i++)
            {
                rdrs[i].Dispose();
                try { File.Delete(paths[i]); }
                catch (Exception ex)
                {
                    Console.Error.WriteLine(string.Format(Resources.CleanupWarning, paths[i], ex.Message));
                }
            }
        }
    }

    // Merges a batch of LZ4 run files into the final plain-text output file.
    private static void MergeBatchToPlainText(List<string> paths, string output)
    {
        var k = paths.Count;
        var rdrs = new PooledByteRunReader[k];
        try
        {
            for (var i = 0; i < k; i++)
                rdrs[i] = new PooledByteRunReader(paths[i], 1024 * 1024);

            var heap = BuildHeap(rdrs, k);

            using var fs = new FileStream(output, FileMode.Create,
                FileAccess.Write, FileShare.None,
                Wbuf, FileOptions.SequentialScan);
            var wb = BytePool.Rent(4 * 1024 * 1024);
            var wOff = 0;
            try
            {
                DrainHeap(heap, rdrs, (buf, start, len) =>
                {
                    if (wOff + len > wb.Length)
                    { fs.Write(wb, 0, wOff); wOff = 0; }
                    Buffer.BlockCopy(buf, start, wb, wOff, len);
                    wOff += len;
                });
                if (wOff > 0) fs.Write(wb, 0, wOff);
            }
            finally { BytePool.Return(wb); }
        }
        finally
        {
            for (var i = 0; i < k; i++)
            {
                rdrs[i].Dispose();
                try { File.Delete(paths[i]); }
                catch (Exception ex)
                {
                    Console.Error.WriteLine(string.Format(Resources.CleanupWarning, paths[i], ex.Message));
                }
            }
        }
    }

    // Shared heap-initialisation logic.
    private static PriorityQueue<ByteMergeEntry, ByteMergeEntry>
        BuildHeap(PooledByteRunReader[] rdrs, int k)
    {
        var heap = new PriorityQueue<ByteMergeEntry, ByteMergeEntry>(
            k, ByteMergeEntryComparer.Instance);
        for (var i = 0; i < k; i++)
            TryEnqueue(heap, rdrs[i], i);
        return heap;
    }

    // Shared drain loop — writes via the supplied action so the caller can
    // target either an LZ4 stream or a plain FileStream.
    private static void DrainHeap(
        PriorityQueue<ByteMergeEntry, ByteMergeEntry> heap,
        PooledByteRunReader[] rdrs,
        Action<byte[], int, int> write)
    {
        while (heap.Count > 0)
        {
            var item = heap.Dequeue();
            write(item.Buffer, item.LineStart, item.LineLength);
            TryEnqueue(heap, rdrs[item.ReaderIndex], item.ReaderIndex);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  ProcessAndSort — parse + build keys + radix sort one chunk
    // ═══════════════════════════════════════════════════════════════════════
    private static SortedChunk ProcessAndSort(byte[] buffer, int length)
    {
        // ── Step 1: parse lines, discover unique text keys ─────────────────
        var capacity = length / 32 + 1000;
        var lineStarts = IntPool.Rent(capacity);
        var lineLengths = IntPool.Rent(capacity);
        var keys = UlongPool.Rent(capacity);
        var indices = IntPool.Rent(capacity);
        var arrCapacity = capacity;

        var lineIdx = 0;
        var currStart = 0;

        // Fix #3: FNV-1a open-addressing hash map, doubles at load 0.5.
        var mapSize = 256;
        var mapMask = mapSize - 1;
        var mapTable = IntPool.Rent(mapSize);  // 0=empty, else uniqueIdx+1
        Array.Clear(mapTable, 0, mapSize);
        var uniqueKeys = new List<byte[]>(64);

        for (var i = 0; i <= length; i++)
        {
            if (i < length && buffer[i] != (byte)'\n') continue;

            var lineLen = i - currStart + (i < length ? 1 : 0);
            if (lineLen == 0) { currStart = i + 1; continue; }

            // Grow per-line arrays if needed
            if (lineIdx >= arrCapacity)
            {
                arrCapacity *= 2;
                lineStarts = GrowIntArray(lineStarts, lineIdx, arrCapacity);
                lineLengths = GrowIntArray(lineLengths, lineIdx, arrCapacity);
                keys = GrowUlongArray(keys, lineIdx, arrCapacity);
                indices = GrowIntArray(indices, lineIdx, arrCapacity);
            }

            lineStarts[lineIdx] = currStart;
            lineLengths[lineIdx] = lineLen;

            // Strip trailing \r\n
            var parseLen = lineLen;
            while (parseLen > 0 &&
                   (buffer[currStart + parseLen - 1] == (byte)'\n' ||
                    buffer[currStart + parseLen - 1] == (byte)'\r'))
                parseLen--;

            var parseSpan = buffer.AsSpan(currStart, parseLen);
            var (keyStart, num) = ParseKeyOffset(parseSpan);
            var keySpan = parseSpan.Slice(keyStart);

            // Grow hash map before load factor reaches 0.5
            if (uniqueKeys.Count >= mapSize / 2)
            {
                var newMapSize = mapSize * 2;
                var newMapMask = newMapSize - 1;
                var newTable = IntPool.Rent(newMapSize);
                Array.Clear(newTable, 0, newMapSize);

                for (var u = 0; u < uniqueKeys.Count; u++)
                {
                    var slot = FnvHashIgnoreCase(uniqueKeys[u].AsSpan()) & newMapMask;
                    while (newTable[slot] != 0)
                        slot = (slot + 1) & newMapMask;
                    newTable[slot] = u + 1;
                }

                IntPool.Return(mapTable);
                mapTable = newTable;
                mapSize = newMapSize;
                mapMask = newMapMask;
            }

            // Lookup or insert
            var hash = FnvHashIgnoreCase(keySpan) & mapMask;
            int rawRank;
            while (true)
            {
                var entry = mapTable[hash];
                if (entry == 0)
                {
                    rawRank = uniqueKeys.Count;
                    uniqueKeys.Add(keySpan.ToArray());
                    mapTable[hash] = rawRank + 1;
                    break;
                }
                if (EqualsIgnoreCase(keySpan, uniqueKeys[entry - 1]))
                {
                    rawRank = entry - 1;
                    break;
                }
                hash = (hash + 1) & mapMask;
            }

            // Store rawRank in high 32 bits; num in low 32 bits.
            // rawRank will be replaced with alphabetical rank in Step 3.
            keys[lineIdx] = ((ulong)(uint)rawRank << 32) | (uint)num;
            indices[lineIdx] = lineIdx;
            lineIdx++;
            currStart = i + 1;
        }

        IntPool.Return(mapTable);

        // ── Step 2: build alphabetical rank mapping ────────────────────────
        var rawStrings = new string[uniqueKeys.Count];
        var sortStrings = new string[uniqueKeys.Count];
        for (var i = 0; i < uniqueKeys.Count; i++)
            rawStrings[i] = sortStrings[i] = Encoding.UTF8.GetString(uniqueKeys[i]);

        Array.Sort(sortStrings, StringComparer.OrdinalIgnoreCase);

        // Fix #2: O(N) dictionary
        var rankDict = new Dictionary<string, int>(
            uniqueKeys.Count, StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < sortStrings.Length; i++)
            rankDict[sortStrings[i]] = i;

        var rankMapping = IntPool.Rent(uniqueKeys.Count);
        for (var i = 0; i < uniqueKeys.Count; i++)
            rankMapping[i] = rankDict[rawStrings[i]];

        // ── Step 3: remap rawRank → alphabetical rank in-place ─────────────
        // Fix #1: rank=32 bits, number=32 bits — no overflow.
        Parallel.ForEach(
            System.Collections.Concurrent.Partitioner.Create(0, lineIdx),
            range =>
            {
                for (var i = range.Item1; i < range.Item2; i++)
                {
                    var val = keys[i];
                    var rr = (int)(val >> 32);
                    var n = (uint)val;
                    keys[i] = ((ulong)(uint)rankMapping[rr] << 32) | n;
                }
            });

        IntPool.Return(rankMapping);

        // ── Step 4: radix sort keys + indices ─────────────────────────────
        var tempKeys = UlongPool.Rent(lineIdx);
        var tempIndices = IntPool.Rent(lineIdx);

        RadixSortWithIndices(keys, indices, tempKeys, tempIndices, lineIdx);

        // Fix #6: keys no longer needed downstream
        UlongPool.Return(keys);
        UlongPool.Return(tempKeys);
        IntPool.Return(tempIndices);

        return new SortedChunk(buffer, lineStarts, lineLengths, indices, lineIdx);
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Radix sort — 4 passes × 16 bits, co-sorts indices[]
    //
    //  Fix #4: ReferenceEquals guard ensures result is always in caller arrays.
    //
    //  Swap trace (src=keys, dst=tempKeys initially):
    //    pass 0: scatter→tempKeys, swap → src=tempKeys, dst=keys
    //    pass 1: scatter→keys,     swap → src=keys,     dst=tempKeys
    //    pass 2: scatter→tempKeys, swap → src=tempKeys, dst=keys
    //    pass 3: scatter→keys,     swap → src=keys,     dst=tempKeys
    //  After pass 3: srcK==keys — result is in keys/indices, no copy needed.
    //  The guard handles any other pass count correctly.
    //
    //  Fix #9: counts[] rented from IntPool instead of heap-allocated.
    //  Saves 256 KB × (run count) of GC pressure — significant for 100 GB.
    // ═══════════════════════════════════════════════════════════════════════
    private static void RadixSortWithIndices(
        ulong[] keys, int[] indices,
        ulong[] tempKeys, int[] tempIndices,
        int count)
    {
        const int bits = 16;
        const int buckets = 1 << bits;  // 65 536
        const int mask = buckets - 1;

        // Fix #9: rent counts array — avoids 256 KB heap alloc per chunk
        var counts = IntPool.Rent(buckets);

        var srcK = keys; var srcI = indices;
        var dstK = tempKeys; var dstI = tempIndices;

        try
        {
            for (var shift = 0; shift < 64; shift += bits)
            {
                Array.Clear(counts, 0, buckets);

                for (var i = 0; i < count; i++)
                    counts[(int)((srcK[i] >> shift) & mask)]++;

                var sum = 0;
                for (var i = 0; i < buckets; i++)
                { var t = counts[i]; counts[i] = sum; sum += t; }

                for (var i = 0; i < count; i++)
                {
                    var bucket = (int)((srcK[i] >> shift) & mask);
                    var dest = counts[bucket]++;
                    dstK[dest] = srcK[i];
                    dstI[dest] = srcI[i];
                }

                var tk = srcK; srcK = dstK; dstK = tk;
                var ti = srcI; srcI = dstI; dstI = ti;
            }
        }
        finally { IntPool.Return(counts); }

        // Fix #4: copy back only if result ended up in temp arrays
        if (!ReferenceEquals(srcK, keys))
        {
            Array.Copy(srcK, keys, count);
            Array.Copy(srcI, indices, count);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  WriteLz4 — Fix #5: no unused `keys` parameter
    // ═══════════════════════════════════════════════════════════════════════
    private static void WriteLz4(
        string path, byte[] buffer,
        int[] starts, int[] lens,
        int[] indices, int count)
    {
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write,
            FileShare.None, Wbuf,
            FileOptions.SequentialScan);
        using var lz4 = LZ4Stream.Encode(fs,
            K4os.Compression.LZ4.LZ4Level.L00_FAST,
            leaveOpen: true);

        var wb = BytePool.Rent(4 * 1024 * 1024);
        var wOff = 0;
        try
        {
            for (var i = 0; i < count; i++)
            {
                var idx = indices[i];
                var start = starts[idx];
                var len = lens[idx];

                if (wOff + len > wb.Length)
                { lz4.Write(wb, 0, wOff); wOff = 0; }

                Buffer.BlockCopy(buffer, start, wb, wOff, len);
                wOff += len;
            }
            if (wOff > 0) lz4.Write(wb, 0, wOff);
        }
        finally { BytePool.Return(wb); }
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Merge helpers
    // ═══════════════════════════════════════════════════════════════════════
    private static void TryEnqueue(
        PriorityQueue<ByteMergeEntry, ByteMergeEntry> heap,
        PooledByteRunReader rdr, int readerIndex)
    {
        if (!rdr.TryReadLine(out var lineStart, out var lineLength)) return;

        var lineSpan = rdr.Buffer.AsSpan(lineStart, lineLength);
        var parseLen = lineLength;
        while (parseLen > 0 &&
               (lineSpan[parseLen - 1] == (byte)'\n' ||
                lineSpan[parseLen - 1] == (byte)'\r'))
            parseLen--;

        var (keyStart, keyLen, num) =
            ParseKeyOffsetForMerge(lineSpan.Slice(0, parseLen));

        var entry = new ByteMergeEntry(
            rdr.Buffer, lineStart + keyStart, keyLen,
            num, lineStart, lineLength, readerIndex);

        heap.Enqueue(entry, entry);
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Array growth helpers
    // ═══════════════════════════════════════════════════════════════════════
    private static int[] GrowIntArray(int[] old, int usedLen, int newCap)
    {
        var next = IntPool.Rent(newCap);
        Buffer.BlockCopy(old, 0, next, 0, usedLen * sizeof(int));
        IntPool.Return(old);
        return next;
    }

    private static ulong[] GrowUlongArray(ulong[] old, int usedLen, int newCap)
    {
        var next = UlongPool.Rent(newCap);
        Buffer.BlockCopy(old, 0, next, 0, usedLen * sizeof(ulong));
        UlongPool.Return(old);
        return next;
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Parsing helpers
    // ═══════════════════════════════════════════════════════════════════════
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int FnvHashIgnoreCase(ReadOnlySpan<byte> span)
    {
        var hash = 2166136261u;
        for (var i = 0; i < span.Length; i++)
        {
            var b = span[i];
            if (b >= 65 && b <= 90) b += 32;
            hash = (hash ^ b) * 16777619u;
        }
        return (int)(hash & 0x7FFFFFFF);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (int keyStart, int num) ParseKeyOffset(ReadOnlySpan<byte> line)
    {
        int num = 0, len = line.Length;
        for (var i = 0; i < len; i++)
        {
            var b = line[i];
            if (b == (byte)'.')
            {
                var ts = i + 1;
                if (ts < len && line[ts] == (byte)' ') ts++;
                return (ts, num);
            }
            var d = (uint)(b - (byte)'0');
            if (d <= 9) num = num * 10 + (int)d;
        }
        return (0, 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (int keyStart, int keyLen, int num)
        ParseKeyOffsetForMerge(ReadOnlySpan<byte> line)
    {
        int num = 0, len = line.Length;
        for (var i = 0; i < len; i++)
        {
            var b = line[i];
            if (b == (byte)'.')
            {
                var ts = i + 1;
                if (ts < len && line[ts] == (byte)' ') ts++;
                return (ts, len - ts, num);
            }
            var d = (uint)(b - (byte)'0');
            if (d <= 9) num = num * 10 + (int)d;
        }
        return (0, 0, 0);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EqualsIgnoreCase(ReadOnlySpan<byte> a, byte[] b)
    {
        if (a.Length != b.Length) return false;
        for (var i = 0; i < a.Length; i++)
        {
            byte c1 = a[i], c2 = b[i];
            if (c1 == c2) continue;
            if (c1 >= 65 && c1 <= 90) c1 += 32;
            if (c2 >= 65 && c2 <= 90) c2 += 32;
            if (c1 != c2) return false;
        }
        return true;
    }

}