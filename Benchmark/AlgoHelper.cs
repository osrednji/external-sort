using System;
using System.IO;

namespace Benchmark;

/// <summary>
/// Provides utility helper functions for algorithm execution, directories creation, and resource cleanup.
/// </summary>
public static class AlgoHelper
{
    /// <summary>
    /// Creates a unique temporary directory under the same parent directory as the target output file.
    /// </summary>
    /// <param name="outputPath">The target final output file path.</param>
    /// <returns>The absolute path to the created temporary directory.</returns>
    public static string MakeTempDir(string outputPath) =>
        Directory.CreateDirectory(Path.Combine(
            Path.GetDirectoryName(Path.GetFullPath(outputPath)) ?? ".",
            "_tmp_" + Guid.NewGuid().ToString("N")[..8])).FullName;

    /// <summary>
    /// Deletes the specified directory recursively. If deletion fails, logs a localized warning instead of crashing.
    /// </summary>
    /// <param name="dir">The directory path to clean up.</param>
    public static void CleanUp(string dir)
    {
        try
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, true);
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(string.Format(Resources.CleanupWarning, dir, ex.Message));
        }
    }
}
