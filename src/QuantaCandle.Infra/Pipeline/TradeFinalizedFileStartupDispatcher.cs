using LogMachina;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Dispatches already-finalized local daily trade files once during recorder startup.
/// </summary>
public sealed class TradeFinalizedFileStartupDispatcher(
    string localRootDirectory,
    ITradeFinalizedFileDispatcher tradeFinalizedFileDispatcher,
    ILogMachina<TradeFinalizedFileStartupDispatcher> log) : ITradeRecorderStartupTask
{
    /// <summary>
    /// Scans each configured instrument directory once and dispatches finalized files in ascending UTC date order.
    /// </summary>
    public async ValueTask Run(IReadOnlyList<Instrument> instruments, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localRootDirectory);
        ArgumentNullException.ThrowIfNull(instruments);

        log.Info($"Trade startup finalized-file discovery begin: instrumentCount={instruments.Count}, localRoot={localRootDirectory}.");

        var seenInstruments = new HashSet<Instrument>();

        foreach (var instrument in instruments)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!seenInstruments.Add(instrument))
            {
                continue;
            }

            var discoveredFiles = TradeLocalDailyFilePath.DiscoverCompleted(localRootDirectory, instrument);
            log.Debug($"Trade startup finalized-file discovery scan: instrument={instrument}, discoveredFileCount={discoveredFiles.Count}.");

            foreach (var discoveredFile in discoveredFiles)
            {
                cancellationToken.ThrowIfCancellationRequested();
                log.Info($"Trade startup finalized-file dispatch: instrument={instrument}, utcDate={discoveredFile.UtcDate:yyyy-MM-dd}, path={discoveredFile.Path}.");
                await tradeFinalizedFileDispatcher.DispatchAsync(instrument, discoveredFile.UtcDate, discoveredFile.Path, cancellationToken).ConfigureAwait(false);
            }
        }

        log.Info("Trade startup finalized-file discovery completed.");
    }
}
