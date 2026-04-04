namespace QuantaCandle.Core.Trading;

/// <summary>
/// Represents the output of a gap scan that only reports detected gaps and affected inputs.
/// </summary>
public sealed record TradeGapScanResult(
    int TotalFilesScanned,
    int TotalTradesScanned,
    int SkippedNonNumericTradeCount,
    IReadOnlyList<TradeGap> DetectedGaps,
    IReadOnlyList<TradeGapAffectedFile> AffectedFiles,
    IReadOnlyList<TradeGapAffectedRange> AffectedRanges)
{
    /// <summary>
    /// Gets the number of JSONL files scanned.
    /// </summary>
    public int TotalFilesScanned { get; } = TotalFilesScanned < 0
        ? throw new ArgumentOutOfRangeException(nameof(TotalFilesScanned), TotalFilesScanned, "TotalFilesScanned cannot be negative.")
        : TotalFilesScanned;

    /// <summary>
    /// Gets the number of trade rows successfully parsed from the scanned files.
    /// </summary>
    public int TotalTradesScanned { get; } = TotalTradesScanned < 0
        ? throw new ArgumentOutOfRangeException(nameof(TotalTradesScanned), TotalTradesScanned, "TotalTradesScanned cannot be negative.")
        : TotalTradesScanned;

    /// <summary>
    /// Gets the number of parsed trades skipped from gap sequencing because the trade identifier was not numeric.
    /// Successful scans keep this value at zero because non-numeric trade identifiers are treated as fatal contract violations.
    /// </summary>
    public int SkippedNonNumericTradeCount { get; } = SkippedNonNumericTradeCount < 0
        ? throw new ArgumentOutOfRangeException(nameof(SkippedNonNumericTradeCount), SkippedNonNumericTradeCount, "SkippedNonNumericTradeCount cannot be negative.")
        : SkippedNonNumericTradeCount;

    /// <summary>
    /// Gets the gaps detected by the scan.
    /// </summary>
    public IReadOnlyList<TradeGap> DetectedGaps { get; } = DetectedGaps ?? throw new ArgumentNullException(nameof(DetectedGaps));

    /// <summary>
    /// Gets the files that contributed to the scan result.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedFile> AffectedFiles { get; } = AffectedFiles ?? throw new ArgumentNullException(nameof(AffectedFiles));

    /// <summary>
    /// Gets the trade ranges that contributed to the scan result.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedRange> AffectedRanges { get; } = AffectedRanges ?? throw new ArgumentNullException(nameof(AffectedRanges));
}
