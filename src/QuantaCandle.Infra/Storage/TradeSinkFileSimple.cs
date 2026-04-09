using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

/// <summary>
/// Uses the finalized local daily file as the terminal destination for file-based recording.
/// </summary>
public sealed class TradeSinkFileSimple : ITradeFinalizedFileDispatcher, ITradeSnapshotFileDispatcher
{
    private readonly TradeSinkFileSimpleOptions _options;

    /// <summary>
    /// Initializes the file-based finalized file dispatcher.
    /// </summary>
    public TradeSinkFileSimple(TradeSinkFileSimpleOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Confirms that the supplied finalized local daily file exists beneath the configured output directory.
    /// </summary>
    public ValueTask DispatchAsync(ExchangeId exchange, Instrument instrument, DateOnly utcDate, string finalizedFilePath, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(finalizedFilePath);
        cancellationToken.ThrowIfCancellationRequested();

        _ = TradeLocalDailyFilePath.ValidateFinalized(_options.OutputDirectory, exchange, instrument, utcDate, finalizedFilePath);

        if (!File.Exists(finalizedFilePath))
        {
            throw new FileNotFoundException("The finalized local daily trade file does not exist.", finalizedFilePath);
        }

        var result = ValueTask.CompletedTask;
        return result;
    }

    /// <summary>
    /// Confirms that the supplied ad-hoc scratch snapshot file exists beneath the configured output directory.
    /// </summary>
    public ValueTask DispatchAsync(ExchangeId exchange, Instrument instrument, string snapshotFilePath, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotFilePath);
        cancellationToken.ThrowIfCancellationRequested();

        _ = TradeLocalDailyFilePath.ValidateSnapshot(_options.OutputDirectory, exchange, instrument, snapshotFilePath);

        if (!File.Exists(snapshotFilePath))
        {
            throw new FileNotFoundException("The ad-hoc local scratch snapshot file does not exist.", snapshotFilePath);
        }

        var result = ValueTask.CompletedTask;
        return result;
    }
}
