using System;
using System.IO;
using K4os.Compression.LZ4.Streams;

namespace Benchmark;

/// <summary>
/// A zero-heap-allocation, high-performance line reader that decodes an LZ4 compressed run file.
/// Utilizes rented byte buffers to scan lines sequentially by scanning for newlines directly in raw bytes.
/// </summary>
public sealed class PooledByteRunReader : IDisposable
{
    private readonly Stream _stream;
    private readonly byte[] _buffer;
    private int _bufferStart;
    private int _bufferEnd;
    private bool _eof;
    private bool _disposed;

    /// <summary>Gets the rented byte buffer used by this reader.</summary>
    public byte[] Buffer => _buffer;

    /// <summary>
    /// Initializes a new instance of the <see cref="PooledByteRunReader"/> class.
    /// Opens the specified file as a sequential read stream, wraps it with an LZ4 decoder, and rents a buffer.
    /// </summary>
    /// <param name="path">The path to the compressed run file.</param>
    /// <param name="bufferSize">The size of the buffer to rent and use for sequential reading.</param>
    public PooledByteRunReader(string path, int bufferSize)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.Read,
            FileShare.Read, bufferSize,
            FileOptions.SequentialScan);
        _stream = LZ4Stream.Decode(fs, leaveOpen: false);
        _buffer = BenchmarkPools.BytePool.Rent(bufferSize);
    }

    /// <summary>
    /// Scans the buffer and retrieves the bounds of the next line, decoding more bytes from the stream when needed.
    /// </summary>
    /// <param name="lineStart">The starting offset of the line within the buffer.</param>
    /// <param name="lineLength">The length of the line in bytes.</param>
    /// <returns>True if a line was successfully scanned; false if the end of the file was reached.</returns>
    public bool TryReadLine(out int lineStart, out int lineLength)
    {
        lineStart = lineLength = 0;
        if (_eof && _bufferStart >= _bufferEnd) return false;

        var searchStart = _bufferStart;
        while (true)
        {
            for (var i = searchStart; i < _bufferEnd; i++)
            {
                if (_buffer[i] != (byte)'\n') continue;
                lineStart = _bufferStart;
                lineLength = i - _bufferStart + 1;
                _bufferStart = i + 1;
                return true;
            }

            if (_eof)
            {
                if (_bufferStart >= _bufferEnd) return false;
                lineStart = _bufferStart;
                lineLength = _bufferEnd - _bufferStart;
                _bufferStart = _bufferEnd;
                return true;
            }

            var leftover = _bufferEnd - _bufferStart;
            if (leftover > 0 && _bufferStart > 0)
                System.Buffer.BlockCopy(
                    _buffer, _bufferStart, _buffer, 0, leftover);
            _bufferStart = 0;
            _bufferEnd = leftover;

            var read = _stream.Read(_buffer, _bufferEnd,
                _buffer.Length - _bufferEnd);
            if (read == 0) _eof = true;
            else _bufferEnd += read;

            searchStart = leftover;
        }
    }

    /// <summary>
    /// Disposes of the underlying stream decoder and returns the rented buffer back to the pool.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _stream.Dispose();
        BenchmarkPools.BytePool.Return(_buffer);
        _disposed = true;
    }
}
