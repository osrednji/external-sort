using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Benchmark;

/// <summary>
/// Implements case-insensitive comparison for <see cref="ByteMergeEntry"/> instances.
/// Orders entries alphabetically by their string key parts, then numerically by their integer prefix value.
/// </summary>
internal sealed class ByteMergeEntryComparer : IComparer<ByteMergeEntry>
{
    /// <summary>A thread-safe singleton instance of the comparer.</summary>
    public static readonly ByteMergeEntryComparer Instance = new();

    /// <summary>
    /// Compares two <see cref="ByteMergeEntry"/> records case-insensitively.
    /// </summary>
    /// <param name="x">The first entry to compare.</param>
    /// <param name="y">The second entry to compare.</param>
    /// <returns>A negative value if x is less than y, zero if they are equal, or a positive value if x is greater than y.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Compare(ByteMergeEntry x, ByteMergeEntry y)
    {
        var c = EqualsIgnoreCaseCompare(
            x.Buffer.AsSpan(x.KeyStart, x.KeyLength),
            y.Buffer.AsSpan(y.KeyStart, y.KeyLength));
        return c != 0 ? c : x.Number.CompareTo(y.Number);
    }
    
    /// <summary>
    /// Performs a high-performance case-insensitive ASCII byte comparison.
    /// Converts uppercase letters [A-Z] (ASCII values 65 to 90) to lowercase by adding 32.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int EqualsIgnoreCaseCompare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        var minLen = Math.Min(a.Length, b.Length);
        for (var i = 0; i < minLen; i++)
        {
            var c1 = a[i];
            var c2 = b[i];
            if (c1 == c2) continue;
            if (c1 >= 65 && c1 <= 90) c1 += 32;
            if (c2 >= 65 && c2 <= 90) c2 += 32;
            if (c1 != c2) return c1 - c2;
        }
        return a.Length - b.Length;
    }
}
