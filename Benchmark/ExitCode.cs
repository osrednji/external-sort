namespace Benchmark;

/// <summary>
/// Defines application exit codes returned by the benchmark process.
/// </summary>
public enum ExitCode
{
    /// <summary>Benchmark completed successfully without errors.</summary>
    Success = 0,

    /// <summary>Validation of configuration settings or input parameters failed.</summary>
    ValidationError = 1,

    /// <summary>An unhandled fatal exception occurred during benchmark execution.</summary>
    FatalError = 4
}
