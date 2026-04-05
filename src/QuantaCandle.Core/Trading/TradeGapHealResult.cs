namespace QuantaCandle.Core.Trading;

/// <summary>
/// Represents the result of healing one bounded local trade gap.
/// </summary>
public sealed record TradeGapHealResult(
    ExchangeId Exchange,
    Instrument Symbol,
    TradeGapHealStatus Outcome,
    MissingTradeIdRange RequestedRange,
    int FetchedTradeCount,
    int InsertedTradeCount,
    bool HasFullRequestedCoverage,
    IReadOnlyList<MissingTradeIdRange> UnresolvedTradeRanges,
    IReadOnlyList<string> Warnings,
    IReadOnlyList<TradeGapAffectedFile> AffectedFiles,
    IReadOnlyList<TradeGapAffectedRange> AffectedRanges)
{
    /// <summary>
    /// Gets the healed exchange.
    /// </summary>
    public ExchangeId Exchange { get; } = Exchange;

    /// <summary>
    /// Gets the healed instrument.
    /// </summary>
    public Instrument Symbol { get; } = Symbol;

    /// <summary>
    /// Gets the high-level healing outcome.
    /// </summary>
    public TradeGapHealStatus Outcome { get; } = Outcome;

    /// <summary>
    /// Gets the requested bounded missing trade identifier range.
    /// </summary>
    public MissingTradeIdRange RequestedRange { get; } = RequestedRange;

    /// <summary>
    /// Gets the count of valid fetched trades accepted from the fetch batch.
    /// </summary>
    public int FetchedTradeCount { get; } = FetchedTradeCount < 0
        ? throw new ArgumentOutOfRangeException(nameof(FetchedTradeCount), FetchedTradeCount, "FetchedTradeCount must be non-negative.")
        : FetchedTradeCount;

    /// <summary>
    /// Gets the count of fetched trades newly inserted into the local dataset.
    /// </summary>
    public int InsertedTradeCount { get; } = InsertedTradeCount < 0
        ? throw new ArgumentOutOfRangeException(nameof(InsertedTradeCount), InsertedTradeCount, "InsertedTradeCount must be non-negative.")
        : InsertedTradeCount;

    /// <summary>
    /// Gets a value indicating whether the fetched batch fully covered the requested range.
    /// </summary>
    public bool HasFullRequestedCoverage { get; } = HasFullRequestedCoverage;

    /// <summary>
    /// Gets unresolved trade identifier ranges derived from the fetched batch only.
    /// </summary>
    public IReadOnlyList<MissingTradeIdRange> UnresolvedTradeRanges { get; } = UnresolvedTradeRanges ?? throw new ArgumentNullException(nameof(UnresolvedTradeRanges));

    /// <summary>
    /// Gets warnings discovered while inspecting the fetched batch.
    /// </summary>
    public IReadOnlyList<string> Warnings { get; } = Warnings ?? throw new ArgumentNullException(nameof(Warnings));

    /// <summary>
    /// Gets the local files touched by the healing attempt.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedFile> AffectedFiles { get; } = AffectedFiles ?? throw new ArgumentNullException(nameof(AffectedFiles));

    /// <summary>
    /// Gets the trade ranges associated with the healing attempt.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedRange> AffectedRanges { get; } = AffectedRanges ?? throw new ArgumentNullException(nameof(AffectedRanges));
}
