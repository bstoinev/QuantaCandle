namespace QuantaCandle.Core.Trading;

/// <summary>
/// Describes the outcome for one gap after a healing attempt.
/// </summary>
public sealed record TradeGapHealingOutcome(TradeGap Gap, string? Details, IReadOnlyList<TradeGapAffectedFile> AffectedFiles, IReadOnlyList<TradeGapAffectedRange> AffectedRanges)
{
    /// <summary>
    /// Gets the gap this outcome corresponds to.
    /// </summary>
    public TradeGap Gap { get; } = Gap ?? throw new ArgumentNullException(nameof(Gap));

    /// <summary>
    /// Gets an optional explanation of the healing outcome.
    /// </summary>
    public string? Details { get; } = string.IsNullOrWhiteSpace(Details) ? null : Details.Trim();

    /// <summary>
    /// Gets the files that were inspected or modified for this gap.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedFile> AffectedFiles { get; } = AffectedFiles ?? throw new ArgumentNullException(nameof(AffectedFiles));

    /// <summary>
    /// Gets the ranges that were inspected or modified for this gap.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedRange> AffectedRanges { get; } = AffectedRanges ?? throw new ArgumentNullException(nameof(AffectedRanges));
}
