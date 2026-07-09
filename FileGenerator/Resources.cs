using System.Resources;

namespace FileGenerator;

public static class Resources
{
    private static readonly ResourceManager ResourceManager = 
        new ResourceManager("FileGenerator.Resources", typeof(Resources).Assembly);

    public static string GetString(string name) => ResourceManager.GetString(name) ?? "";

    public static string Generating => GetString("Generating");
    public static string Target => GetString("Target");
    public static string Progress => GetString("Progress");
    public static string Done => GetString("Done");
    public static string RunningWithConfig => GetString("RunningWithConfig");
    public static string OutputPath => GetString("OutputPath");
    public static string SizeMb => GetString("SizeMb");
    public static string Seed => GetString("Seed");

    public static string ConfigMissingDefaultPath => GetString("ConfigMissingDefaultPath");
    public static string ConfigMissingSizeMb => GetString("ConfigMissingSizeMb");
    public static string ConfigMissingSeed => GetString("ConfigMissingSeed");
    public static string ConfigMissingStrings => GetString("ConfigMissingStrings");
    public static string ConfigEmptyStrings => GetString("ConfigEmptyStrings");
}
