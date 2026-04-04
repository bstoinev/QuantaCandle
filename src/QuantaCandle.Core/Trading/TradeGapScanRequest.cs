namespace QuantaCandle.Core.Trading;

/// <summary>
/// Defines the scope for a gap scan without implying that healing will happen.
/// </summary>
public sealed record TradeGapScanRequest(ExchangeId Exchange, Instrument Symbol, IReadOnlyList<TradeGapAffectedFile> CandidateFiles, IReadOnlyList<TradeGapAffectedRange> CandidateRanges)
{
    /// <summary>
    /// Gets the exchange to scan.
    /// </summary>
    public ExchangeId Exchange { get; } = Exchange;

    /// <summary>
    /// Gets the instrument to scan.
    /// </summary>
    public Instrument Symbol { get; } = Symbol;

    /// <summary>
    /// Gets the candidate files to inspect when the scan is file-backed.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedFile> CandidateFiles { get; } = CandidateFiles ?? throw new ArgumentNullException(nameof(CandidateFiles));

    /// <summary>
    /// Gets the candidate trade ranges to inspect when the scan is range-backed.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedRange> CandidateRanges { get; } = CandidateRanges ?? throw new ArgumentNullException(nameof(CandidateRanges));
}
