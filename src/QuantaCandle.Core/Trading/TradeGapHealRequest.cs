namespace QuantaCandle.Core.Trading;

/// <summary>
/// Defines a healing request for one exchange instrument scope and a concrete set of gaps.
/// </summary>
public sealed record TradeGapHealRequest(ExchangeId Exchange, Instrument Symbol, IReadOnlyList<TradeGap> Gaps, IReadOnlyList<TradeGapAffectedFile> CandidateFiles, IReadOnlyList<TradeGapAffectedRange> CandidateRanges)
{
    /// <summary>
    /// Gets the exchange whose gaps should be healed.
    /// </summary>
    public ExchangeId Exchange { get; } = Exchange;

    /// <summary>
    /// Gets the instrument whose gaps should be healed.
    /// </summary>
    public Instrument Symbol { get; } = Symbol;

    /// <summary>
    /// Gets the gaps selected for healing.
    /// </summary>
    public IReadOnlyList<TradeGap> Gaps { get; } = Gaps ?? throw new ArgumentNullException(nameof(Gaps));

    /// <summary>
    /// Gets the candidate files the healer may inspect or rewrite.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedFile> CandidateFiles { get; } = CandidateFiles ?? throw new ArgumentNullException(nameof(CandidateFiles));

    /// <summary>
    /// Gets the candidate trade ranges the healer may inspect or rewrite.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedRange> CandidateRanges { get; } = CandidateRanges ?? throw new ArgumentNullException(nameof(CandidateRanges));
}
