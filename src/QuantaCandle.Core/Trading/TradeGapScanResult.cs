namespace QuantaCandle.Core.Trading;

/// <summary>
/// Represents the output of a gap scan that only reports detected gaps and affected inputs.
/// </summary>
public sealed record TradeGapScanResult(ExchangeId Exchange, Instrument Symbol, IReadOnlyList<TradeGap> DetectedGaps, IReadOnlyList<TradeGapAffectedFile> AffectedFiles, IReadOnlyList<TradeGapAffectedRange> AffectedRanges)
{
    /// <summary>
    /// Gets the scanned exchange.
    /// </summary>
    public ExchangeId Exchange { get; } = Exchange;

    /// <summary>
    /// Gets the scanned instrument.
    /// </summary>
    public Instrument Symbol { get; } = Symbol;

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
