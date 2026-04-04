namespace QuantaCandle.Core.Trading;

/// <summary>
/// Describes one file that was scanned or modified during gap scan or healing work.
/// </summary>
public sealed record TradeGapAffectedFile(string Path, DateOnly? TradingDay)
{
    /// <summary>
    /// Gets the repository- or storage-relative path of the affected file.
    /// </summary>
    public string Path { get; } = string.IsNullOrWhiteSpace(Path)
        ? throw new ArgumentException("Path cannot be null or whitespace.", nameof(Path))
        : Path.Trim();

    /// <summary>
    /// Gets the trading day represented by the file when that mapping is known.
    /// </summary>
    public DateOnly? TradingDay { get; } = TradingDay;
}
