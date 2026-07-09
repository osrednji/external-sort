using System;
using System.IO;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace FileGenerator;

/// <summary>
/// Generates large test files in "Number. String\n" format.
///
/// Design goals:
///   - High write throughput via 64 MB buffered StreamWriter
///   - Realistic distribution: ~30 distinct strings, many duplicates (as per spec)
///   - Numbers in [1, int.MaxValue), randomly distributed
///   - Deterministic output for a given seed (default seed=42)
/// </summary>
public static class FileGenerator
{
    public static async Task GenerateAsync(string outputPath, long targetBytes, System.Collections.Generic.IReadOnlyList<string> strings, int seed = 42)
    {
        Console.WriteLine(string.Format(Resources.Generating, outputPath));
        Console.WriteLine(string.Format(Resources.Target, targetBytes / (1024.0 * 1024.0)));

        var parentDir = Path.GetDirectoryName(Path.GetFullPath(outputPath));
        if (!string.IsNullOrEmpty(parentDir))
        {
            Directory.CreateDirectory(parentDir);
        }

        var rng = new Random(seed);
        long written = 0L;
        long lines   = 0L;

        const int bufSize = 64 * 1024 * 1024; // 64 MB write buffer
        await using var fs     = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
            FileShare.None, bufSize, FileOptions.SequentialScan);
        await using var writer = new StreamWriter(fs, Encoding.UTF8, bufSize);

        while (written < targetBytes)
        {
            // Bias toward numbers < 100_000 so duplicates (same string + same number) are common
            int    num  = rng.Next(1, 100_000) < 80_000
                            ? rng.Next(1, 100_000)
                            : rng.Next(100_000, int.MaxValue);
            string text = strings[rng.Next(strings.Count)];
            string line = $"{num}. {text}";

            await writer.WriteLineAsync(line);
            written += Encoding.UTF8.GetByteCount(line) + 1;
            lines++;

            if (lines % 5_000_000 == 0)
                Console.WriteLine(string.Format(Resources.Progress, 100.0 * written / targetBytes, lines));
        }

        Console.WriteLine(string.Format(Resources.Done, lines, written / (1024.0 * 1024.0)));
    }

    public static async Task Main(string[] args)
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        string rawPath = config["DefaultPath"] ?? throw new InvalidOperationException(Resources.ConfigMissingDefaultPath);
        string defaultPath = Path.IsPathRooted(rawPath)
            ? rawPath
            : Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, rawPath));

        long defaultSizeMb = long.Parse(config["SizeMb"] ?? throw new InvalidOperationException(Resources.ConfigMissingSizeMb));
        int defaultSeed = int.Parse(config["Seed"] ?? throw new InvalidOperationException(Resources.ConfigMissingSeed));

        var strings = config.GetSection("Strings").GetChildren().Select(c => c.Value).Where(v => v != null).Cast<string>().ToArray();
        if (strings == null || strings.Length == 0)
        {
            throw new InvalidOperationException(Resources.ConfigMissingStrings);
        }
        if (strings.Any(string.IsNullOrWhiteSpace))
        {
            throw new InvalidOperationException(Resources.ConfigEmptyStrings);
        }

        string path = defaultPath;
        long sizeMb = defaultSizeMb;
        int seed = defaultSeed;

        if (args.Length >= 1)
        {
            if (long.TryParse(args[0], out long parsedSize))
            {
                // The first argument is a number, so treat it as sizeMb
                sizeMb = parsedSize;
                if (args.Length >= 2)
                {
                    int.TryParse(args[1], out seed);
                }
            }
            else
            {
                // The first argument is a path
                if (!string.IsNullOrWhiteSpace(args[0]))
                {
                    path = args[0];
                }
                if (args.Length >= 2)
                {
                    long.TryParse(args[1], out sizeMb);
                }
                if (args.Length >= 3)
                {
                    int.TryParse(args[2], out seed);
                }
            }
        }

        Console.WriteLine(Resources.RunningWithConfig);
        Console.WriteLine(string.Format(Resources.OutputPath, path));
        Console.WriteLine(string.Format(Resources.SizeMb, sizeMb, sizeMb / 1024.0));
        Console.WriteLine(string.Format(Resources.Seed, seed));
        Console.WriteLine();

        await GenerateAsync(path, sizeMb * 1024 * 1024, strings, seed);
    }
}
