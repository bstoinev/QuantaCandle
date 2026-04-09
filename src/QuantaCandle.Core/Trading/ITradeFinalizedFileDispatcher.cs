namespace QuantaCandle.Core.Trading;

/// <summary>
/// Dispatches an already finalized local UTC day trade file to its destination.
/// </summary>
public interface ITradeFinalizedFileDispatcher
{
    /// <summary>
    /// Dispatches the supplied finalized local daily file.
    /// </summary>
    ValueTask DispatchAsync(ExchangeId exchange, Instrument instrument, DateOnly utcDate, string finalizedFilePath, CancellationToken cancellationToken);
}
