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
    public sealed record CompletedDayLocalFile(ExchangeId Exchange, Instrument Instrument, DateOnly UtcDate, string Path);

    /// <summary>
    /// Builds the local JSONL path for one instrument UTC day.
    /// </summary>
    public static string Build(string localRootDirectory, ExchangeId exchange, Instrument instrument, DateOnly utcDate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localRootDirectory);

        var result = Path.Combine(localRootDirectory, exchange.ToString(), instrument.ToString(), $"{utcDate:yyyy-MM-dd}.jsonl");
        return result;
    }

    /// <summary>
    /// Builds the local scratch JSONL path for one instrument.
    /// </summary>
    public static string BuildScratch(string localRootDirectory, ExchangeId exchange, Instrument instrument)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localRootDirectory);

        var result = Path.Combine(localRootDirectory, exchange.ToString(), instrument.ToString(), "qc-scratch.jsonl");
        return result;
    }

    /// <summary>
    /// Builds the local JSONL path for one ad-hoc UTC timestamped scratch snapshot.
    /// </summary>
    public static string BuildSnapshot(string localRootDirectory, ExchangeId exchange, Instrument instrument, DateTimeOffset utcTimestamp)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localRootDirectory);

        var snapshotFileName = $"{utcTimestamp.UtcDateTime:yyyy-MM-dd.HHmmssfff}.jsonl";
        var result = Path.Combine(localRootDirectory, exchange.ToString(), instrument.ToString(), snapshotFileName);
        return result;
    }

    /// <summary>
    /// Validates that the supplied path matches the finalized local JSONL path for the specified instrument UTC day.
    /// </summary>
    public static string ValidateFinalized(string localRootDirectory, ExchangeId exchange, Instrument instrument, DateOnly utcDate, string finalizedFilePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localRootDirectory);
        ArgumentException.ThrowIfNullOrWhiteSpace(finalizedFilePath);

        var expectedPath = Build(localRootDirectory, exchange, instrument, utcDate);
        if (!string.Equals(expectedPath, finalizedFilePath, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Finalized file path must match the configured output directory. Expected '{expectedPath}', actual '{finalizedFilePath}'.");
        }

        var result = expectedPath;
        return result;
    }

    /// <summary>
    /// Validates that the supplied path matches the ad-hoc scratch snapshot layout for the specified instrument.
    /// </summary>
    public static string ValidateSnapshot(string localRootDirectory, ExchangeId exchange, Instrument instrument, string snapshotFilePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localRootDirectory);
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotFilePath);

        var expectedDirectory = Path.Combine(localRootDirectory, exchange.ToString(), instrument.ToString());
        var actualDirectory = Path.GetDirectoryName(snapshotFilePath);
        if (!string.Equals(expectedDirectory, actualDirectory, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Snapshot file path must match the configured output directory. Expected directory '{expectedDirectory}', actual '{actualDirectory}'.");
        }

        var snapshotFileName = Path.GetFileNameWithoutExtension(snapshotFilePath);
        var isSnapshotFileName = DateTime.TryParseExact(
            snapshotFileName,
            "yyyy-MM-dd.HHmmssfff",
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out _);

        if (!isSnapshotFileName)
        {
            throw new InvalidOperationException($"Snapshot file path must use the UTC timestamp snapshot naming convention. Actual '{snapshotFilePath}'.");
        }

        return snapshotFilePath;
    }

    /// <summary>
    /// Discovers completed local day files for one configured instrument directory.
    /// </summary>
    public static IReadOnlyList<CompletedDayLocalFile> DiscoverCompleted(string localRootDirectory, ExchangeId exchange, Instrument instrument)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localRootDirectory);

        var result = new List<CompletedDayLocalFile>();
        var instrumentDirectory = Path.Combine(localRootDirectory, exchange.ToString(), instrument.ToString());

        if (!Directory.Exists(instrumentDirectory))
        {
            return result;
        }

        foreach (var filePath in Directory.EnumerateFiles(instrumentDirectory, "*.jsonl", SearchOption.TopDirectoryOnly))
        {
            var fileName = Path.GetFileName(filePath);
            if (string.Equals(fileName, "qc-scratch.jsonl", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(filePath);
            if (!DateOnly.TryParseExact(fileNameWithoutExtension, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var utcDate))
            {
                continue;
            }

            result.Add(new CompletedDayLocalFile(exchange, instrument, utcDate, filePath));
        }

        result = result
            .OrderBy(item => item.UtcDate)
            .ToList();

        return result;
    }
}
