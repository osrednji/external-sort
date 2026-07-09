using System.Resources;

namespace Benchmark;

public static class Resources
{
    private static readonly ResourceManager ResourceManager = 
        new ResourceManager("Benchmark.Resources", typeof(Resources).Assembly);

    public static string GetString(string name) => ResourceManager.GetString(name) ?? "";

    public static string NoCustomInput => GetString("NoCustomInput");
    public static string DefaultInput => GetString("DefaultInput");
    public static string DefaultRuns => GetString("DefaultRuns");
    public static string DefaultWorkDir => GetString("DefaultWorkDir");
    public static string DefaultReportPath => GetString("DefaultReportPath");
    public static string ErrorInputNotFound => GetString("ErrorInputNotFound");
    public static string ErrorRunsMustBePositive => GetString("ErrorRunsMustBePositive");
    public static string FatalError => GetString("FatalError");
    public static string HeaderBorder => GetString("HeaderBorder");
    public static string HeaderTitle => GetString("HeaderTitle");
    public static string HeaderTitleStart => GetString("HeaderTitleStart");
    public static string HeaderInput => GetString("HeaderInput");
    public static string HeaderAlgorithmsCount => GetString("HeaderAlgorithmsCount");
    public static string HeaderRunsPerAlgo => GetString("HeaderRunsPerAlgo");
    public static string HeaderTotalRuns => GetString("HeaderTotalRuns");
    public static string HeaderCpuCores => GetString("HeaderCpuCores");
    public static string AlgoSectionHeader => GetString("AlgoSectionHeader");
    public static string AlgoDescription => GetString("AlgoDescription");
    public static string AlgoRunsCount => GetString("AlgoRunsCount");
    public static string AlgoSeparator => GetString("AlgoSeparator");
    public static string RunProgressLine => GetString("RunProgressLine");
    public static string RunTime => GetString("RunTime");
    public static string RunFailed => GetString("RunFailed");
    public static string AlgoSummaryMedian => GetString("AlgoSummaryMedian");
    public static string AlgoSummaryMinMax => GetString("AlgoSummaryMinMax");
    public static string AlgoSummaryFailed => GetString("AlgoSummaryFailed");
    public static string AlgoSectionFooter => GetString("AlgoSectionFooter");
    public static string FullReportWritten => GetString("FullReportWritten");
    public static string ProgressLine => GetString("ProgressLine");

    public static string ReportHeaderBorderTop => GetString("ReportHeaderBorderTop");
    public static string ReportHeaderTitle => GetString("ReportHeaderTitle");
    public static string ReportHeaderBorderBottom => GetString("ReportHeaderBorderBottom");
    public static string ReportDate => GetString("ReportDate");
    public static string ReportInputFile => GetString("ReportInputFile");
    public static string ReportFileSize => GetString("ReportFileSize");
    public static string ReportFileSizeUnavailable => GetString("ReportFileSizeUnavailable");
    public static string ReportRunsPerAlgo => GetString("ReportRunsPerAlgo");
    public static string ReportCpuCores => GetString("ReportCpuCores");
    public static string ReportDotNetVersion => GetString("ReportDotNetVersion");
    public static string ReportSectionBorder => GetString("ReportSectionBorder");
    public static string ReportPerAlgoResultsTitle => GetString("ReportPerAlgoResultsTitle");
    public static string ReportAlgoName => GetString("ReportAlgoName");
    public static string ReportAlgoDesc => GetString("ReportAlgoDesc");
    public static string ReportAlgoSuccess => GetString("ReportAlgoSuccess");
    public static string ReportAlgoMedian => GetString("ReportAlgoMedian");
    public static string ReportAlgoMin => GetString("ReportAlgoMin");
    public static string ReportAlgoMax => GetString("ReportAlgoMax");
    public static string ReportAlgoAvg => GetString("ReportAlgoAvg");
    public static string ReportAlgoStdDev => GetString("ReportAlgoStdDev");
    public static string ReportRunByRunHeader => GetString("ReportRunByRunHeader");
    public static string ReportRunFailed => GetString("ReportRunFailed");
    public static string ReportRunLine => GetString("ReportRunLine");
    public static string ReportSummaryTitle => GetString("ReportSummaryTitle");
    public static string ReportNoSuccessfulRuns => GetString("ReportNoSuccessfulRuns");
    public static string ReportHeaderCols => GetString("ReportHeaderCols");
    public static string ColHeaderAlgorithm => GetString("ColHeaderAlgorithm");
    public static string ColHeaderMedian => GetString("ColHeaderMedian");
    public static string ColHeaderVsFastest => GetString("ColHeaderVsFastest");
    public static string ColHeaderVsPrevious => GetString("ColHeaderVsPrevious");
    public static string ReportBestMarker => GetString("ReportBestMarker");
    public static string ReportSlowerMarker => GetString("ReportSlowerMarker");
    public static string ReportNoPrevMarker => GetString("ReportNoPrevMarker");
    public static string ReportVsPrevMarker => GetString("ReportVsPrevMarker");
    public static string ReportFastestIndicator => GetString("ReportFastestIndicator");
    public static string ReportRankLine => GetString("ReportRankLine");
    public static string ReportFastestSummary => GetString("ReportFastestSummary");
    public static string ReportSpeedupSummary => GetString("ReportSpeedupSummary");
    public static string ReportEndOfReport => GetString("ReportEndOfReport");

    public static string ConfigMissingInputPath => GetString("ConfigMissingInputPath");
    public static string ConfigMissingRuns => GetString("ConfigMissingRuns");
    public static string ConfigMissingWorkDir => GetString("ConfigMissingWorkDir");
    public static string ConfigMissingResultPath => GetString("ConfigMissingResultPath");

    public static string CleanupWarning => GetString("CleanupWarning");
    public static string PeakMemory => GetString("PeakMemory");
}
