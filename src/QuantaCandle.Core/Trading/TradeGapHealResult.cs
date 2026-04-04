namespace QuantaCandle.Core.Trading;

/// <summary>
/// Represents the output of a healing run with healed and unresolved gaps separated explicitly.
/// </summary>
public sealed record TradeGapHealResult(ExchangeId Exchange, Instrument Symbol, IReadOnlyList<TradeGapHealingOutcome> HealedGaps, IReadOnlyList<TradeGapHealingOutcome> UnresolvedGaps, IReadOnlyList<TradeGapAffectedFile> AffectedFiles, IReadOnlyList<TradeGapAffectedRange> AffectedRanges)
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
    /// Gets the gaps that were healed successfully.
    /// </summary>
    public IReadOnlyList<TradeGapHealingOutcome> HealedGaps { get; } = HealedGaps ?? throw new ArgumentNullException(nameof(HealedGaps));

    /// <summary>
    /// Gets the gaps that remain unresolved after healing.
    /// </summary>
    public IReadOnlyList<TradeGapHealingOutcome> UnresolvedGaps { get; } = UnresolvedGaps ?? throw new ArgumentNullException(nameof(UnresolvedGaps));

    /// <summary>
    /// Gets the files touched by the healing run overall.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedFile> AffectedFiles { get; } = AffectedFiles ?? throw new ArgumentNullException(nameof(AffectedFiles));

    /// <summary>
    /// Gets the trade ranges touched by the healing run overall.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedRange> AffectedRanges { get; } = AffectedRanges ?? throw new ArgumentNullException(nameof(AffectedRanges));
}
