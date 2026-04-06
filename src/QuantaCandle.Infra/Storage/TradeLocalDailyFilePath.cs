using System.Globalization;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

/// <summary>
/// Builds deterministic local daily trade file paths.
/// </summary>
public static class TradeLocalDailyFilePath
{
    /// <summary>
    /// Represents one completed local instrument-day file discovered on disk.
    /// </summary>
    public sealed record CompletedDayLocalFile(Instrument Instrument, DateOnly UtcDate, string Path);

    /// <summary>
    /// Builds the local JSONL path for one instrument UTC day.
    /// </summary>
    public static string Build(string localRootDirectory, Instrument instrument, DateOnly utcDate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localRootDirectory);

        var result = Path.Combine(localRootDirectory, instrument.ToString(), $"{utcDate:yyyy-MM-dd}.jsonl");
        return result;
    }

    /// <summary>
    /// Discovers completed local day files across all instrument folders beneath the local root directory.
    /// </summary>
    public static IReadOnlyList<CompletedDayLocalFile> DiscoverCompleted(string localRootDirectory, DateOnly activeUtcDate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localRootDirectory);

        var result = new List<CompletedDayLocalFile>();

        if (!Directory.Exists(localRootDirectory))
        {
            return result;
        }

        foreach (var instrumentDirectory in Directory.EnumerateDirectories(localRootDirectory))
        {
            var instrumentName = Path.GetFileName(instrumentDirectory);
            if (string.IsNullOrWhiteSpace(instrumentName))
            {
                continue;
            }

            Instrument instrument;
            try
            {
                instrument = Instrument.Parse(instrumentName);
            }
            catch (ArgumentException)
            {
                continue;
            }

            foreach (var filePath in Directory.EnumerateFiles(instrumentDirectory, "*.jsonl", SearchOption.TopDirectoryOnly))
            {
                var fileName = Path.GetFileNameWithoutExtension(filePath);
                if (!DateOnly.TryParseExact(fileName, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var utcDate))
                {
                    continue;
                }

                if (utcDate >= activeUtcDate)
                {
                    continue;
                }

                result.Add(new CompletedDayLocalFile(instrument, utcDate, filePath));
            }
        }

        result = result
            .OrderBy(item => item.Instrument.ToString(), StringComparer.Ordinal)
            .ThenBy(item => item.UtcDate)
            .ToList();

        return result;
    }
}
