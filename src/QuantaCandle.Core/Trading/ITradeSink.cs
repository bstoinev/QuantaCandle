namespace QuantaCandle.Core.Trading;

public interface ITradeSink
{
    /// <summary>
    /// Appends trades idempotently, deduplicating by <see cref="TradeKey"/>.
    /// </summary>
    /// <remarks>
    /// Expected error semantics:
    /// - Throws <see cref="OperationCanceledException"/> when <paramref name="cancellationToken"/> is canceled.
    /// - Throws exceptions for persistence failures (connectivity, constraint violations beyond deduplication, etc.).
    /// </remarks>
    ValueTask<TradeAppendResult> Append(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken);
}
