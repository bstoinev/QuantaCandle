using QuantaCandle.Core;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra;

/// <summary>
/// Buffers active UTC trade days in memory, checkpoints them to local daily files, and uploads finalized daily files to S3.
/// </summary>
public sealed class TradeSinkS3Simple : ITradeSink, ITradeSinkLifecycle
{
    private readonly TradeSinkS3SimpleOptions options;
    private readonly IS3ObjectUploader uploader;
    private readonly IClock clock;
    private readonly SemaphoreSlim stateGate = new(1, 1);
    private readonly Dictionary<(Instrument Instrument, DateOnly UtcDate), List<TradeInfo>> bufferedTradesByDay = [];
    private DateTimeOffset nextCheckpointAtUtc;

    public TradeSinkS3Simple(TradeSinkS3SimpleOptions options, IS3ObjectUploader uploader, IClock clock)
    {
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.uploader = uploader ?? throw new ArgumentNullException(nameof(uploader));
        this.clock = clock ?? throw new ArgumentNullException(nameof(clock));

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
    public async ValueTask CheckpointActive(CancellationToken cancellationToken)
    {
        if (clock.UtcNow < nextCheckpointAtUtc)
        {
            return;
        }

        await stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (clock.UtcNow >= nextCheckpointAtUtc)
            {
                await PersistBufferedDaysAsync(finalizeCompletedDays: true, uploadCompletedDays: true, cancellationToken).ConfigureAwait(false);
                nextCheckpointAtUtc = clock.UtcNow + options.CheckpointInterval;
            }
        }
        finally
        {
            stateGate.Release();
        }
    }

    /// <summary>
    /// Flushes all active in-memory day buffers to local daily files without uploading incomplete days to S3.
    /// </summary>
    public async ValueTask FlushOnShutdown(CancellationToken cancellationToken)
    {
        await stateGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await PersistBufferedDaysAsync(finalizeCompletedDays: false, uploadCompletedDays: false, cancellationToken).ConfigureAwait(false);
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

    private async Task PersistBufferedDaysAsync(bool finalizeCompletedDays, bool uploadCompletedDays, CancellationToken cancellationToken)
    {
        var currentUtcDate = DateOnly.FromDateTime(clock.UtcNow.UtcDateTime);
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
            await TradeJsonlFile.WriteFullPayloadAsync(localPath, payload, cancellationToken).ConfigureAwait(false);

            var isCompletedDay = key.UtcDate < currentUtcDate;
            if (finalizeCompletedDays && isCompletedDay)
            {
                if (uploadCompletedDays)
                {
                    var objectKey = TradeSinkS3DailyObjectKey.Build(options.Prefix, key.Instrument.ToString(), key.UtcDate);
                    await uploader.UploadTextAsync(options.BucketName, objectKey, payload, cancellationToken).ConfigureAwait(false);
                }

                bufferedTradesByDay.Remove(key);
            }
        }
    }
}
