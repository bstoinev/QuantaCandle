using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Runs recorder startup work once before live trade collection begins.
/// </summary>
public interface ITradeRecorderStartupTask
{
    /// <summary>
    /// Executes startup processing for the configured instruments before live ingest starts.
    /// </summary>
    ValueTask Run(IReadOnlyList<Instrument> instruments, CancellationToken cancellationToken);
}
