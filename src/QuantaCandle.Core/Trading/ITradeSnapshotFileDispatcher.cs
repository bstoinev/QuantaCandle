namespace QuantaCandle.Core.Trading;

/// <summary>
/// Dispatches an ad-hoc local scratch snapshot file to its destination.
/// </summary>
public interface ITradeSnapshotFileDispatcher
{
    /// <summary>
    /// Dispatches the supplied ad-hoc scratch snapshot file.
    /// </summary>
    ValueTask DispatchAsync(Instrument instrument, string snapshotFilePath, CancellationToken cancellationToken);
}
