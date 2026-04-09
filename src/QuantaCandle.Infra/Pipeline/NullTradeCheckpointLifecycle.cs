using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Disables recorder-side checkpoint persistence when no local persistence root is configured.
/// </summary>
public sealed class NullTradeCheckpointLifecycle : ITradeCheckpointLifecycle
{
    /// <summary>
    /// Ignores tracked trades.
    /// </summary>
    public ValueTask<int> TrackAppendedTrades(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
    {
        return ValueTask.FromResult(0);
    }

    /// <summary>
    /// Completes without persisting any checkpoint state.
    /// </summary>
    public ValueTask<bool> CheckpointActive(CancellationToken cancellationToken)
    {
        return ValueTask.FromResult(false);
    }

    /// <summary>
    /// Completes without exporting any snapshot state.
    /// </summary>
    public ValueTask<bool> DispatchActiveSnapshot(CancellationToken cancellationToken)
    {
        return ValueTask.FromResult(false);
    }

    /// <summary>
    /// Completes without persisting any checkpoint state.
    /// </summary>
    public ValueTask FlushOnShutdown(CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }
}
