using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace Benchmark;

class Program
{
    static async Task<int> Main(string[] args)
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        string rawInputPath = config["InputPath"] ?? throw new InvalidOperationException(Resources.ConfigMissingInputPath);
        string defaultInputPath = Path.IsPathRooted(rawInputPath)
            ? rawInputPath
            : Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, rawInputPath));

        int defaultRuns = int.Parse(config["Runs"] ?? throw new InvalidOperationException(Resources.ConfigMissingRuns));

        string rawWorkDir = config["WorkDir"] ?? throw new InvalidOperationException(Resources.ConfigMissingWorkDir);
        string defaultWorkDir = Path.IsPathRooted(rawWorkDir)
            ? rawWorkDir
            : Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, rawWorkDir));

        string rawResultPath = config["ResultPath"] ?? throw new InvalidOperationException(Resources.ConfigMissingResultPath);
        string defaultResultPath = Path.IsPathRooted(rawResultPath)
            ? rawResultPath
            : Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, rawResultPath));

        var inputPath  = args.Length >= 1 ? args[0] : defaultInputPath;
        var runs       = args.Length >= 2 ? int.Parse(args[1]) : defaultRuns;
        var workDir    = args.Length >= 3 ? args[2] : defaultWorkDir;
        var resultPath = args.Length >= 4 ? args[3] : defaultResultPath;

        if (args.Length < 1)
        {
            Console.WriteLine(Resources.NoCustomInput);
            Console.WriteLine(string.Format(Resources.DefaultInput, inputPath));
            Console.WriteLine(string.Format(Resources.DefaultRuns, runs));
            Console.WriteLine(string.Format(Resources.DefaultWorkDir, workDir));
            Console.WriteLine(string.Format(Resources.DefaultReportPath, resultPath));
            Console.WriteLine();
        }

        if (!File.Exists(inputPath))
        {
            Console.Error.WriteLine(string.Format(Resources.ErrorInputNotFound, inputPath));
            return (int)ExitCode.ValidationError;
        }

        if (runs < 1)
        {
            Console.Error.WriteLine(Resources.ErrorRunsMustBePositive);
            return (int)ExitCode.ValidationError;
        }

        var algorithms = new List<ISortAlgorithm>
        { 
            new SortingAlgorithm()
        };
        
        try
        {
            await BenchmarkRunner.RunAsync(algorithms, inputPath, workDir, resultPath, runs);
            return (int)ExitCode.Success;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(string.Format(Resources.FatalError, ex));
            return (int)ExitCode.FatalError;
        }
    }
}