namespace QuantaCandle.Core.Trading;

/// <summary>
/// Describes the file and line location of a trade that contributed to a detected gap boundary.
/// </summary>
public sealed record TradeGapBoundaryLocation(string FilePath, int LineNumber)
{
    /// <summary>
    /// Gets the storage-relative path of the file that contained the trade.
    /// </summary>
    public string FilePath { get; } = string.IsNullOrWhiteSpace(FilePath)
        ? throw new ArgumentException("FilePath cannot be null or whitespace.", nameof(FilePath))
        : FilePath.Trim();

    /// <summary>
    /// Gets the 1-based line number of the trade inside the file.
    /// </summary>
    public int LineNumber { get; } = LineNumber <= 0
        ? throw new ArgumentOutOfRangeException(nameof(LineNumber), LineNumber, "LineNumber must be positive.")
        : LineNumber;
}
