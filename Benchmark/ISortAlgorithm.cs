using System.Threading.Tasks;

namespace Benchmark;

/// <summary>
/// Defines the contract that every sorting algorithm benchmark candidate must implement.
/// </summary>
public interface ISortAlgorithm
{
    /// <summary>Gets the display name of the sorting algorithm.</summary>
    string Name { get; }

    /// <summary>Gets a detailed description of the sorting strategy and characteristics.</summary>
    string Description { get; }

    /// <summary>
    /// Sorts the input file and writes the sorted results to the specified output file path.
    /// </summary>
    /// <param name="inputPath">The absolute path to the unsorted source text file.</param>
    /// <param name="outputPath">The absolute path where the sorted text file should be created.</param>
    /// <returns>A task representing the asynchronous sorting operation.</returns>
    Task SortAsync(string inputPath, string outputPath);
}
