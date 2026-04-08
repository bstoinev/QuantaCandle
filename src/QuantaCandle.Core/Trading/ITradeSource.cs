namespace QuantaCandle.Core.Trading;

public interface ITradeSource
{
    ExchangeId Exchange { get; }

    /// <summary>
    /// Streams live trades until cancellation. Implementations may reconnect transparently.
    /// </summary>
    /// <remarks>
    /// Expected error semantics:
    /// - Throws <see cref="OperationCanceledException"/> when <paramref name="cancellationToken"/> is canceled.
    /// - Throws other exceptions for unrecoverable failures; transient errors may be handled by reconnecting.
    /// </remarks>
    IAsyncEnumerable<TradeInfo> GetLiveTrades(Instrument symbol, CancellationToken cancellationToken);

    /// <summary>
    /// Returns historical trades after <paramref name="fromExclusive"/> (if provided), typically in ascending order.
    /// </summary>
    /// <remarks>
    /// Expected error semantics:
    /// - Throws <see cref="OperationCanceledException"/> when <paramref name="cancellationToken"/> is canceled.
    /// - Throws exceptions for transport/protocol failures.
    /// </remarks>
    IAsyncEnumerable<TradeInfo> GetBackfillTrades(Instrument symbol, TradeWatermark? fromExclusive, CancellationToken cancellationToken);
}
