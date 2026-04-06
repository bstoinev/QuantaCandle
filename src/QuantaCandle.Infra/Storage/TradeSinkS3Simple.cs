using LogMachina;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

/// <summary>
/// Writes trades to deterministic local day files and uploads completed UTC days to S3.
/// </summary>
public sealed class TradeSinkS3Simple : ITradeSink
{
    private readonly ILogMachina<TradeSinkS3Simple> _log;
    private readonly TradeSinkS3SimpleOptions _options;
    private readonly IS3ObjectUploader _uploader;
    private readonly IClock _clock;

    /// <summary>
    /// Initializes the S3 trade sink.
    /// </summary>
    public TradeSinkS3Simple(TradeSinkS3SimpleOptions options, IS3ObjectUploader uploader, IClock clock, ILogMachina<TradeSinkS3Simple> log)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _uploader = uploader ?? throw new ArgumentNullException(nameof(uploader));
        _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        _log = log ?? throw new ArgumentNullException(nameof(log));

        if (string.IsNullOrWhiteSpace(_options.BucketName))
        {
            throw new ArgumentException("BucketName is required for the S3 trade sink.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(_options.LocalRootDirectory))
        {
            throw new ArgumentException("LocalRootDirectory is required for the S3 trade sink.", nameof(options));
        }
    }

    /// <summary>
    /// Appends trades to deterministic local day files and uploads any completed UTC day files.
    /// </summary>
    public async ValueTask<TradeAppendResult> Append(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
    {
        var insertedCount = trades.Count;
        if (insertedCount == 0)
        {
            return new TradeAppendResult(InsertedCount: 0, DuplicateCount: 0);
        }

        var byPath = new Dictionary<string, List<TradeInfo>>(StringComparer.OrdinalIgnoreCase);

        foreach (var trade in trades)
        {
            var utcDate = DateOnly.FromDateTime(trade.Timestamp.UtcDateTime);
            var localPath = TradeLocalDailyFilePath.Build(_options.LocalRootDirectory, trade.Key.Symbol, utcDate);

            if (!byPath.TryGetValue(localPath, out var list))
            {
                list = [];
                byPath[localPath] = list;
            }

            list.Add(trade);
        }

    /// <summary>
    /// Persists active-day checkpoints when due and finalizes any completed UTC days.
    /// </summary>
    public async ValueTask<bool> CheckpointActive(CancellationToken cancellationToken)
    {
        var result = false;

        if (clock.UtcNow >= nextCheckpointAtUtc)
        {
            await stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (clock.UtcNow >= nextCheckpointAtUtc)
                {
                    var currentUtcDate = DateOnly.FromDateTime(clock.UtcNow.UtcDateTime);
                    log.Info($"{CheckpointTickLogPrefix}: now={clock.UtcNow:O}, activeUtcDate={currentUtcDate:yyyy-MM-dd}.");
                    await PersistBufferedDays(currentUtcDate, cancellationToken).ConfigureAwait(false);
                    await UploadCompletedDays(currentUtcDate, cancellationToken).ConfigureAwait(false);
                    nextCheckpointAtUtc = clock.UtcNow + options.CheckpointInterval;
                    result = true;
                }
            }
            finally
            {
                stateGate.Release();
            }
        }

        return result;
    }

    /// <summary>
    /// Flushes all active in-memory day buffers to local daily files without uploading incomplete days to S3.
    /// </summary>
    public async ValueTask FlushOnShutdown(CancellationToken cancellationToken)
    {
        await stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var currentUtcDate = DateOnly.FromDateTime(clock.UtcNow.UtcDateTime);
            await PersistBufferedDays(currentUtcDate, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            stateGate.Release();
        }
    }

    private async Task<List<TradeInfo>> GetOrCreateBufferAsync((Instrument Instrument, DateOnly UtcDate) key, CancellationToken cancellationToken)
    {
        if (!bufferedTradesByDay.TryGetValue(key, out var result))
        {
            var localPath = TradeLocalDailyFilePath.Build(options.LocalRootDirectory, key.Instrument, key.UtcDate);
            var recoveredTrades = await TradeJsonlFile.ReadTradesAsync(localPath, cancellationToken).ConfigureAwait(false);
            result = recoveredTrades.ToList();
            bufferedTradesByDay[key] = result;
        }

        return result;
    }

    /// <summary>
    /// Writes every buffered instrument-day payload to its deterministic local checkpoint file.
    /// </summary>
    private async Task PersistBufferedDays(DateOnly currentUtcDate, CancellationToken cancellationToken)
    {
        var orderedKeys = bufferedTradesByDay.Keys
            .OrderBy(item => item.Instrument.ToString(), StringComparer.Ordinal)
            .ThenBy(item => item.UtcDate)
            .ToList();

        foreach (var key in orderedKeys)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var sortedTrades = bufferedTradesByDay[key]
                .OrderBy(trade => trade.Timestamp)
                .ThenBy(trade => trade.Key.TradeId, StringComparer.Ordinal)
                .ToList();

        return new TradeAppendResult(insertedCount, DuplicateCount: 0);
    }

    /// <summary>
    /// Discovers completed day local files, uploads them to S3, and deletes them from local storage only after success.
    /// </summary>
    private async Task UploadCompletedDays(DateOnly currentUtcDate, CancellationToken cancellationToken)
    {
        var discoveredFiles = TradeLocalDailyFilePath.DiscoverCompleted(_options.LocalRootDirectory, currentUtcDate);

        foreach (var discoveredFile in discoveredFiles)
        {
            cancellationToken.ThrowIfCancellationRequested();

            _log.Info($"Trade S3 discovered completed local file: instrument={discoveredFile.Instrument}, utcDate={discoveredFile.UtcDate:yyyy-MM-dd}, path={discoveredFile.Path}.");

            var payload = await File.ReadAllTextAsync(discoveredFile.Path, cancellationToken).ConfigureAwait(false);
            var objectKey = TradeSinkS3DailyObjectKey.Build(_options.Prefix, discoveredFile.Instrument.ToString(), discoveredFile.UtcDate);
            _log.Info($"Trade S3 upload start: bucket={_options.BucketName}, objectKey={objectKey}, path={discoveredFile.Path}.");

            try
            {
                await _uploader.UploadTextAsync(_options.BucketName, objectKey, payload, cancellationToken).ConfigureAwait(false);
                _log.Info($"Trade S3 upload success: bucket={_options.BucketName}, objectKey={objectKey}.");
                File.Delete(discoveredFile.Path);
                _log.Info($"Trade S3 local delete after upload: path={discoveredFile.Path}.");
            }
            catch (Exception ex)
            {
                _log.Warn($"Trade S3 upload failure: bucket={_options.BucketName}, objectKey={objectKey}, path={discoveredFile.Path}.");
                _log.Error(ex);
                throw;
            }
        }
    }
}
