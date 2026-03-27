using System;

namespace QuantaCandle.Core.Trading;

/// <summary>
/// Represents a persisted recorder-side trade gap for one exchange instrument stream.
/// </summary>
public sealed record TradeGap
{
    /// <summary>
    /// Creates a gap in the observed/open state when only the lower durable boundary is known.
    /// </summary>
    public static TradeGap CreateOpen(
        Guid gapId,
        ExchangeId exchange,
        Instrument symbol,
        TradeWatermark fromExclusive,
        DateTimeOffset observedAt)
    {
        var gap = new TradeGap(gapId, exchange, symbol, TradeGapStatus.Open, fromExclusive, null, null, observedAt, null);
        return gap;
    }

    /// <summary>
    /// Initializes a persisted trade gap snapshot.
    /// </summary>
    public TradeGap(
        Guid gapId,
        ExchangeId exchange,
        Instrument symbol,
        TradeGapStatus status,
        TradeWatermark fromExclusive,
        TradeWatermark? toInclusive,
        MissingTradeIdRange? missingTradeIds,
        DateTimeOffset observedAt,
        DateTimeOffset? resolvedAt)
    {
        if (gapId == Guid.Empty)
        {
            throw new ArgumentException("GapId must be non-empty.", nameof(gapId));
        }

        if (observedAt == default)
        {
            throw new ArgumentException("ObservedAt must be non-default.", nameof(observedAt));
        }

        if (status == TradeGapStatus.Open && toInclusive is not null)
        {
            throw new ArgumentException("Open gaps cannot have an upper boundary.", nameof(toInclusive));
        }

        if (status == TradeGapStatus.Open && missingTradeIds is not null)
        {
            throw new ArgumentException("Open gaps cannot have a missing trade id range.", nameof(missingTradeIds));
        }

        if (status != TradeGapStatus.Open && toInclusive is null)
        {
            throw new ArgumentException("Bounded and resolved gaps require an upper boundary.", nameof(toInclusive));
        }

        if (status == TradeGapStatus.Resolved && resolvedAt is null)
        {
            throw new ArgumentException("Resolved gaps require a resolved timestamp.", nameof(resolvedAt));
        }

        if (status != TradeGapStatus.Resolved && resolvedAt is not null)
        {
            throw new ArgumentException("Only resolved gaps may set a resolved timestamp.", nameof(resolvedAt));
        }

        GapId = gapId;
        Exchange = exchange;
        Symbol = symbol;
        Status = status;
        FromExclusive = fromExclusive;
        ToInclusive = toInclusive;
        MissingTradeIds = missingTradeIds;
        ObservedAt = observedAt;
        ResolvedAt = resolvedAt;
    }

    public Guid GapId { get; }

    public ExchangeId Exchange { get; }

    public Instrument Symbol { get; }

    public TradeGapStatus Status { get; }

    public TradeWatermark FromExclusive { get; }

    public TradeWatermark? ToInclusive { get; }

    public MissingTradeIdRange? MissingTradeIds { get; }

    public DateTimeOffset ObservedAt { get; }

    public DateTimeOffset? ResolvedAt { get; }

    /// <summary>
    /// Moves the gap into the bounded state once the upper boundary becomes known.
    /// </summary>
    public TradeGap ToBounded(TradeWatermark toInclusive, MissingTradeIdRange? missingTradeIds)
    {
        var gap = new TradeGap(GapId, Exchange, Symbol, TradeGapStatus.Bounded, FromExclusive, toInclusive, missingTradeIds, ObservedAt, null);
        return gap;
    }

    /// <summary>
    /// Marks the gap as resolved after a later recovery step succeeds.
    /// </summary>
    public TradeGap ToResolved(DateTimeOffset resolvedAt)
    {
        if (ToInclusive is null)
        {
            throw new InvalidOperationException("Cannot resolve a gap without an upper boundary.");
        }

        var gap = new TradeGap(GapId, Exchange, Symbol, TradeGapStatus.Resolved, FromExclusive, ToInclusive, MissingTradeIds, ObservedAt, resolvedAt);
        return gap;
    }
}
