namespace QuantaCandle.Core.Trading;

/// <summary>
/// Describes one trade gap download progress update.
/// </summary>
public sealed record TradeGapProgressUpdate(
    string Stage,
    long CompletedTradeCount,
    long TotalTradeCount,
    int DownloadedPageCount,
    bool IsCompleted)
{
    /// <summary>
    /// Gets the current stage label.
    /// </summary>
    public string Stage { get; } = string.IsNullOrWhiteSpace(Stage)
        ? throw new ArgumentException("Stage cannot be null or whitespace.", nameof(Stage))
        : Stage.Trim();

    /// <summary>
    /// Gets the completed trade count.
    /// </summary>
    public long CompletedTradeCount { get; } = CompletedTradeCount < 0
        ? throw new ArgumentOutOfRangeException(nameof(CompletedTradeCount), CompletedTradeCount, "CompletedTradeCount must be non-negative.")
        : CompletedTradeCount;

    /// <summary>
    /// Gets the total trade count.
    /// </summary>
    public long TotalTradeCount { get; } = TotalTradeCount < 0
        ? throw new ArgumentOutOfRangeException(nameof(TotalTradeCount), TotalTradeCount, "TotalTradeCount must be non-negative.")
        : TotalTradeCount;

    /// <summary>
    /// Gets the downloaded page count.
    /// </summary>
    public int DownloadedPageCount { get; } = DownloadedPageCount < 0
        ? throw new ArgumentOutOfRangeException(nameof(DownloadedPageCount), DownloadedPageCount, "DownloadedPageCount must be non-negative.")
        : DownloadedPageCount;

    /// <summary>
    /// Gets a value indicating whether the reported progress is final.
    /// </summary>
    public bool IsCompleted { get; } = IsCompleted;
}
