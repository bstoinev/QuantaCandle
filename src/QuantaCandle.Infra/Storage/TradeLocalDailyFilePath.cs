using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

/// <summary>
/// Builds deterministic local daily trade file paths.
/// </summary>
public static class TradeLocalDailyFilePath
{
    /// <summary>
    /// Builds the local JSONL path for one instrument UTC day.
    /// </summary>
    public static string Build(string localRootDirectory, Instrument instrument, DateOnly utcDate)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localRootDirectory);

        var result = Path.Combine(localRootDirectory, instrument.ToString(), $"{utcDate:yyyy-MM-dd}.jsonl");
        return result;
    }
}
