using System;

namespace Benchmark;

/// <summary>
/// Represents the result metrics of a single execution run of a sorting algorithm benchmark.
/// </summary>
/// <param name="AlgorithmName">The name of the algorithm that was executed.</param>
/// <param name="RunNumber">The iteration index (1-based) of the run.</param>
/// <param name="Elapsed">The total time elapsed during execution.</param>
/// <param name="Success">A flag indicating whether the run completed successfully without errors.</param>
/// <param name="Error">The exception message if the run failed; otherwise null.</param>
public record RunResult(string AlgorithmName, int RunNumber, TimeSpan Elapsed, bool Success, string? Error = null);
