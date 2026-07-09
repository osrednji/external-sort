using System.Buffers;

namespace Benchmark;

/// <summary>
/// Provides shared high-performance array pools used during sorting and merging
/// to minimize garbage collection overhead and memory allocations.
/// </summary>
public static class BenchmarkPools
{
    /// <summary>
    /// Gets a shared <see cref="ArrayPool{Byte}"/> pool for raw line byte chunks,
    /// sorting prefix mappings, and stream compression buffers.
    /// </summary>
    public static readonly ArrayPool<byte> BytePool = ArrayPool<byte>.Shared;

    /// <summary>
    /// Gets a shared <see cref="ArrayPool{Int32}"/> pool for Radix sort offset indexing,
    /// boundary indexes, and merge index mappings.
    /// </summary>
    public static readonly ArrayPool<int> IndexPool = ArrayPool<int>.Shared;

    /// <summary>
    /// Gets a shared <see cref="ArrayPool{Int32}"/> pool for general integer arrays.
    /// </summary>
    public static readonly ArrayPool<int> IntPool = ArrayPool<int>.Shared;

    /// <summary>
    /// Gets a shared <see cref="ArrayPool{UInt64}"/> pool for bit-packed sorting keys.
    /// </summary>
    public static readonly ArrayPool<ulong> UlongPool = ArrayPool<ulong>.Shared;
}
