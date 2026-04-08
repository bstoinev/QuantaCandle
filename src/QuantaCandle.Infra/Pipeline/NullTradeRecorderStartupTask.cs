using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Performs no recorder startup work.
/// </summary>
public sealed class NullTradeRecorderStartupTask : ITradeRecorderStartupTask
{
    /// <summary>
    /// Completes immediately without performing any startup processing.
    /// </summary>
    public ValueTask Run(IReadOnlyList<Instrument> instruments, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(instruments);
        cancellationToken.ThrowIfCancellationRequested();

        var result = ValueTask.CompletedTask;
        return result;
    }
}
