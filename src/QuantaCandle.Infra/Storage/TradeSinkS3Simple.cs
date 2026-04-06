using LogMachina;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

/// <summary>
/// Buffers active UTC trade days in memory, checkpoints them to local daily files, and uploads finalized daily files to S3.
/// </summary>
public sealed class TradeSinkS3Simple : ITradeSink, ITradeSinkLifecycle
{
    private const string CheckpointTickLogPrefix = "Trade S3 checkpoint tick";

    private readonly ILogMachina<TradeSinkS3Simple> log;
    private readonly TradeSinkS3SimpleOptions options;
    private readonly IS3ObjectUploader uploader;
    private readonly IClock clock;
    private readonly SemaphoreSlim stateGate = new(1, 1);
    private readonly Dictionary<(Instrument Instrument, DateOnly UtcDate), List<TradeInfo>> bufferedTradesByDay = [];
    private DateTimeOffset nextCheckpointAtUtc;

    public TradeSinkS3Simple(TradeSinkS3SimpleOptions options, IS3ObjectUploader uploader, IClock clock, ILogMachina<TradeSinkS3Simple> log)
    {
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.uploader = uploader ?? throw new ArgumentNullException(nameof(uploader));
        this.clock = clock ?? throw new ArgumentNullException(nameof(clock));
        this.log = log ?? throw new ArgumentNullException(nameof(log));

        if (string.IsNullOrWhiteSpace(this.options.BucketName))
        {
            throw new ArgumentException("BucketName is required for the S3 trade sink.", nameof(options));
        }

        if (string.IsNullOrWhiteSpace(this.options.LocalRootDirectory))
        {
            throw new ArgumentException("LocalRootDirectory is required for the S3 trade sink.", nameof(options));
        }

        if (this.options.CheckpointInterval <= TimeSpan.Zero)
        {
            throw new ArgumentException("CheckpointInterval must be greater than zero.", nameof(options));
        }

        nextCheckpointAtUtc = this.clock.UtcNow + this.options.CheckpointInterval;
    }

    public async ValueTask<TradeAppendResult> Append(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
    {
        var insertedCount = trades.Count;
        if (insertedCount == 0)
        {
            return new TradeAppendResult(InsertedCount: 0, DuplicateCount: 0);
        }

        await stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            foreach (var trade in trades)
            {
                var bufferKey = (trade.Key.Symbol, DateOnly.FromDateTime(trade.Timestamp.UtcDateTime));
                var dailyTrades = await GetOrCreateBufferAsync(bufferKey, cancellationToken).ConfigureAwait(false);

                dailyTrades.Add(trade);
            }
        }
        finally
        {
            stateGate.Release();
        }

        return new TradeAppendResult(insertedCount, DuplicateCount: 0);
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

            var payload = TradeJsonlFile.BuildPayload(sortedTrades);
            var localPath = TradeLocalDailyFilePath.Build(options.LocalRootDirectory, key.Instrument, key.UtcDate);
            log.Info($"Trade S3 local checkpoint write: instrument={key.Instrument}, utcDate={key.UtcDate:yyyy-MM-dd}, path={localPath}.");
            await TradeJsonlFile.WriteFullPayloadAsync(localPath, payload, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Discovers completed day local files, uploads them to S3, and deletes them from local storage only after success.
    /// </summary>
    private async Task UploadCompletedDays(DateOnly currentUtcDate, CancellationToken cancellationToken)
    {
        var discoveredFiles = TradeLocalDailyFilePath.DiscoverCompleted(options.LocalRootDirectory, currentUtcDate);
        var uploadedKeys = new HashSet<(Instrument Instrument, DateOnly UtcDate)>();

        foreach (var discoveredFile in discoveredFiles)
        {
            cancellationToken.ThrowIfCancellationRequested();

            log.Info($"Trade S3 discovered completed local file: instrument={discoveredFile.Instrument}, utcDate={discoveredFile.UtcDate:yyyy-MM-dd}, path={discoveredFile.Path}.");

            var uploadKey = (discoveredFile.Instrument, discoveredFile.UtcDate);
            if (!uploadedKeys.Add(uploadKey))
            {
                continue;
            }

            var payload = await File.ReadAllTextAsync(discoveredFile.Path, cancellationToken).ConfigureAwait(false);
            var objectKey = TradeSinkS3DailyObjectKey.Build(options.Prefix, discoveredFile.Instrument.ToString(), discoveredFile.UtcDate);
            log.Info($"Trade S3 upload start: bucket={options.BucketName}, objectKey={objectKey}, path={discoveredFile.Path}.");

            try
            {
                await uploader.UploadTextAsync(options.BucketName, objectKey, payload, cancellationToken).ConfigureAwait(false);
                log.Info($"Trade S3 upload success: bucket={options.BucketName}, objectKey={objectKey}.");
                File.Delete(discoveredFile.Path);
                log.Info($"Trade S3 local delete after upload: path={discoveredFile.Path}.");
                bufferedTradesByDay.Remove(uploadKey);
            }
            catch (Exception ex)
            {
                log.Warn($"Trade S3 upload failure: bucket={options.BucketName}, objectKey={objectKey}, path={discoveredFile.Path}.");
                log.Error(ex);
                throw;
            }
        }
    }
}
