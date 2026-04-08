using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

/// <summary>
/// Ignores finalized local daily files.
/// </summary>
public sealed class TradeSinkNull : ITradeFinalizedFileDispatcher
{
    /// <summary>
    /// Ignores the supplied finalized file.
    /// </summary>
    public ValueTask DispatchAsync(Instrument instrument, DateOnly utcDate, string finalizedFilePath, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var result = ValueTask.CompletedTask;
        return result;
    }
}
