using System.Text.Json;
using System.Threading.Channels;

using LogMachina;

using Moq;

using QuantaCandle.Core;
using QuantaCandle.Core.Trading;
using QuantaCandle.Infra;
using QuantaCandle.Infra.Options;
using QuantaCandle.Infra.Pipeline;
using QuantaCandle.Infra.Storage;

namespace QuantaCandle.CLI.Tests;

/// <summary>
/// Validates the reusable recorder and generator flow without depending on executable projects.
/// </summary>
public sealed class ExecutableFlowValidationTests
{
    [Fact]
    public async Task CollectThenGenerateCreatesCandleFilesWithoutNetworkDependency()
    {
        var root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        var tradeDirectory = Path.Combine(root, "trade-data");

        Directory.CreateDirectory(tradeDirectory);

        try
        {
            await CollectTradesAsync(tradeDirectory);

            var tradeFiles = Directory.GetFiles(tradeDirectory, "*.jsonl", SearchOption.AllDirectories);
            Assert.NotEmpty(tradeFiles);

            await RewriteExchangeToBinanceAsync(tradeFiles);
            MoveTradesIntoExchangeDirectory(tradeFiles, tradeDirectory, "binance");

            var result = await TradeToCandleGenerator.Run(new CliOptions(CliMode.Candlize, root, "binance", "BTC-USDT", "1m", [], "csv"), CancellationToken.None);

            Assert.True(result.InputTradeCount > 0);

            var candleInstrumentDirectory = Path.Combine(root, "candle-data", "binance", "BTC-USDT");
            var candleFiles = Directory.GetFiles(candleInstrumentDirectory, "*.csv", SearchOption.AllDirectories);
            Assert.NotEmpty(candleFiles);

            var firstFile = candleFiles.OrderBy(path => path, StringComparer.Ordinal).First();
            var lines = await File.ReadAllLinesAsync(firstFile, CancellationToken.None);

            Assert.NotEmpty(lines);
            Assert.Equal("OpenTimeUtc,Instrument,Open,High,Low,Close,BaseVolume,QuoteVolume,BuyQuoteVolume,SellQuoteVolume,TradeCount", lines[0]);
            Assert.True(lines.Length >= 2);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    private static async Task CollectTradesAsync(string tradeDirectory)
    {
        var collectorOptions = new CollectorOptions(
            ["BTC-USDT"],
            ChannelCapacity: 1024,
            BatchSize: 1,
            FlushInterval: TimeSpan.FromMilliseconds(100),
            CheckpointInterval: TimeSpan.FromMilliseconds(100),
            MaxTradesPerSecond: 20);
        var stats = new TradePipelineStats();
        var stateStore = new InMemoryIngestionStateStore();
        var deduplicator = new InMemoryTradeDeduplicator(collectorOptions);
        var sink = new TradeSinkFileSimple(new TradeSinkFileSimpleOptions(tradeDirectory));
        var logMoq = new Mock<ILogMachina<TradeIngestWorker>>();
        var clockMoq = new Mock<IClock>();
        clockMoq
            .SetupGet(mock => mock.UtcNow)
            .Returns(new DateTimeOffset(2026, 3, 11, 23, 59, 59, 500, TimeSpan.Zero));
        var checkpointLifecycleLogMoq = new Mock<ILogMachina<TradeScratchCheckpointLifecycle>>();
        var checkpointLifecycle = new TradeScratchCheckpointLifecycle(
            clockMoq.Object,
            tradeDirectory,
            sink,
            sink,
            stateStore,
            new LocalFileTradeGapScanner(),
            null,
            null,
            checkpointLifecycleLogMoq.Object);

        var worker = new TradeIngestWorker(stateStore, new CheckpointSignal(), checkpointLifecycle, new TradeCheckpointTriggerOptions(1024), deduplicator, stats, logMoq.Object);
        var source = new TradeSourceStub(new TradeSourceStubOptions(new ExchangeId("Stub"), 20, 50_000m, 0.01m, 0.001m), clockMoq.Object);

        using var sourceCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(1_500));

        var channel = BoundedChannelFactory.CreateTradeChannel(collectorOptions.ChannelCapacity);
        var ingestTask = worker.Run(channel.Reader, collectorOptions, CancellationToken.None);
        var collectTask = WriteTradesAsync(source, channel.Writer, collectorOptions.Instruments[0], sourceCts.Token);

        try
        {
            await collectTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            channel.Writer.TryComplete();
        }

        await ingestTask.ConfigureAwait(false);

        Assert.True(stats.GetSnapshot().TradesWritten > 0);
    }

    private static async Task WriteTradesAsync(
        ITradeSource source,
        ChannelWriter<TradeInfo> writer,
        Instrument instrument,
        CancellationToken cancellationToken)
    {
        await foreach (var trade in source.GetLiveTrades(instrument, cancellationToken).ConfigureAwait(false))
        {
            await writer.WriteAsync(trade, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task RewriteExchangeToBinanceAsync(IEnumerable<string> files)
    {
        foreach (var file in files.OrderBy(path => path, StringComparer.Ordinal))
        {
            var lines = await File.ReadAllLinesAsync(file, CancellationToken.None).ConfigureAwait(false);
            var rewrittenLines = new List<string>(lines.Length);

            foreach (var line in lines)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                using var doc = JsonDocument.Parse(line);
                var root = doc.RootElement;

                var instrument = root.GetProperty("instrument").GetString() ?? string.Empty;
                var tradeId = root.GetProperty("tradeId").GetString() ?? string.Empty;
                var timestamp = root.GetProperty("timestamp").GetDateTimeOffset();
                var price = root.GetProperty("price").GetDecimal();
                var quantity = root.GetProperty("quantity").GetDecimal();

                var rewritten = JsonSerializer.Serialize(new
                {
                    exchange = "binance",
                    instrument,
                    tradeId,
                    timestamp,
                    price,
                    quantity,
                });

                rewrittenLines.Add(rewritten);
            }

            var payload = string.Join(Environment.NewLine, rewrittenLines) + Environment.NewLine;
            await File.WriteAllTextAsync(file, payload, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static void MoveTradesIntoExchangeDirectory(IEnumerable<string> files, string tradeDirectory, string exchange)
    {
        var exchangeDirectory = Path.Combine(tradeDirectory, exchange);
        Directory.CreateDirectory(exchangeDirectory);

        foreach (var file in files.OrderBy(path => path, StringComparer.Ordinal))
        {
            var instrumentDirectoryName = Path.GetFileName(Path.GetDirectoryName(file)) ?? string.Empty;
            var destinationDirectory = Path.Combine(exchangeDirectory, instrumentDirectoryName);
            Directory.CreateDirectory(destinationDirectory);

            var destinationPath = Path.Combine(destinationDirectory, Path.GetFileName(file));
            File.Move(file, destinationPath, overwrite: true);
        }
    }
}
