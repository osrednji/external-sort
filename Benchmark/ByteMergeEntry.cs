namespace Benchmark;

/// <summary>
/// Represents a parsed line entry stored as raw bytes, used during K-way merging of sorted chunks.
/// </summary>
public readonly struct ByteMergeEntry
{
    /// <summary>The shared byte buffer containing the line data.</summary>
    public readonly byte[] Buffer;

    /// <summary>The starting index of the string key part within the buffer.</summary>
    public readonly int KeyStart;

    /// <summary>The byte length of the string key part.</summary>
    public readonly int KeyLength;

    /// <summary>The integer prefix value of the parsed line.</summary>
    public readonly int Number;

    /// <summary>The starting index of the complete line (including integer and string parts) within the buffer.</summary>
    public readonly int LineStart;

    /// <summary>The byte length of the complete line.</summary>
    public readonly int LineLength;

    /// <summary>The index of the reader stream from which this line entry was read.</summary>
    public readonly int ReaderIndex;

    /// <summary>
    /// Initializes a new instance of the <see cref="ByteMergeEntry"/> struct.
    /// </summary>
    /// <param name="buffer">The shared byte buffer containing the line data.</param>
    /// <param name="keyStart">The starting index of the string key part.</param>
    /// <param name="keyLength">The byte length of the string key part.</param>
    /// <param name="number">The integer prefix value of the parsed line.</param>
    /// <param name="lineStart">The starting index of the complete line.</param>
    /// <param name="lineLength">The byte length of the complete line.</param>
    /// <param name="readerIndex">The index of the source reader stream.</param>
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
