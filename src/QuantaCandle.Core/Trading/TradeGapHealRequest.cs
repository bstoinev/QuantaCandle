namespace QuantaCandle.Core.Trading;

/// <summary>
/// Defines one bounded local trade gap healing request for a specific exchange and instrument dataset.
/// </summary>
public sealed record TradeGapHealRequest(
    string RootDirectory,
    ExchangeId Exchange,
    Instrument Symbol,
    long MissingTradeIdStart,
    long MissingTradeIdEnd,
    IReadOnlyList<TradeGapAffectedFile> CandidateFiles,
    TradeGapAffectedRange? AffectedRange = null,
    IReadOnlyList<MissingTradeIdRange>? RequestedMissingTradeRanges = null,
    ITradeGapProgressReporter? ProgressReporter = null)
{
    /// <summary>
    /// Gets the local dataset root directory that contains the instrument JSONL files.
    /// </summary>
    public string RootDirectory { get; } = string.IsNullOrWhiteSpace(RootDirectory)
        ? throw new ArgumentException("RootDirectory cannot be null or whitespace.", nameof(RootDirectory))
        : RootDirectory.Trim();

    /// <summary>
    /// Gets the exchange whose gap should be healed.
    /// </summary>
    public ExchangeId Exchange { get; } = Exchange;

    /// <summary>
    /// Gets the instrument whose gap should be healed.
    /// </summary>
    public Instrument Symbol { get; } = Symbol;

    /// <summary>
    /// Gets the inclusive missing trade identifier range start.
    /// </summary>
    public long MissingTradeIdStart { get; } = MissingTradeIdStart <= 0
        ? throw new ArgumentOutOfRangeException(nameof(MissingTradeIdStart), MissingTradeIdStart, "MissingTradeIdStart must be positive.")
        : MissingTradeIdStart;

    /// <summary>
    /// Gets the inclusive missing trade identifier range end.
    /// </summary>
    public long MissingTradeIdEnd { get; } = MissingTradeIdEnd < MissingTradeIdStart
        ? throw new ArgumentOutOfRangeException(nameof(MissingTradeIdEnd), MissingTradeIdEnd, "MissingTradeIdEnd must be greater than or equal to MissingTradeIdStart.")
        : MissingTradeIdEnd;

    /// <summary>
    /// Gets the candidate files the healer may inspect or rewrite.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedFile> CandidateFiles { get; } = CandidateFiles ?? throw new ArgumentNullException(nameof(CandidateFiles));

    /// <summary>
    /// Gets the optional affected range metadata that identified this bounded gap.
    /// </summary>
    public TradeGapAffectedRange? AffectedRange { get; } = AffectedRange;

    /// <summary>
    /// Gets the exact missing trade identifier ranges that should be considered unresolved when healing finishes.
    /// </summary>
    public IReadOnlyList<MissingTradeIdRange> RequestedMissingTradeRanges { get; } = NormalizeRequestedMissingTradeRanges(
        MissingTradeIdStart,
        MissingTradeIdEnd,
        RequestedMissingTradeRanges);

    /// <summary>
    /// Gets the optional progress reporter for the current healing pass.
    /// </summary>
    public ITradeGapProgressReporter? ProgressReporter { get; } = ProgressReporter;

    private static IReadOnlyList<MissingTradeIdRange> NormalizeRequestedMissingTradeRanges(
        long missingTradeIdStart,
        long missingTradeIdEnd,
        IReadOnlyList<MissingTradeIdRange>? requestedMissingTradeRanges)
    {
        var result = requestedMissingTradeRanges is null || requestedMissingTradeRanges.Count == 0
            ? new[] { new MissingTradeIdRange(missingTradeIdStart, missingTradeIdEnd) }
            : requestedMissingTradeRanges.MergeContiguous();

        foreach (var range in result)
        {
            if (range.FirstTradeId < missingTradeIdStart || range.LastTradeId > missingTradeIdEnd)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(RequestedMissingTradeRanges),
                    $"Requested missing trade range {range.FirstTradeId}-{range.LastTradeId} falls outside fetch envelope {missingTradeIdStart}-{missingTradeIdEnd}.");
            }
        }

        return result;
    }
}
