using System.Reflection;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Tests.Storage;

public sealed class TradeSinkFileSimpleTests
{
    private static readonly ExchangeId StubExchange = new("Stub");

    [Fact]
    public async Task DispatchUsesExistingFinalizedDailyFile()
    {
        var root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));

        try
        {
            var sink = new TradeSinkFileSimple(new TradeSinkFileSimpleOptions(root));
            var instrument = Instrument.Parse("BTC-USDT");
            var utcDate = new DateOnly(2026, 3, 12);
            var finalizedPath = TradeLocalDailyFilePath.Build(root, StubExchange, instrument, utcDate);
            var payload = TradeJsonlFile.BuildPayload([CreateTrade("123", utcDate)]);

            Directory.CreateDirectory(Path.GetDirectoryName(finalizedPath)!);
            await File.WriteAllTextAsync(finalizedPath, payload, CancellationToken.None);

            await sink.DispatchAsync(StubExchange, instrument, utcDate, finalizedPath, CancellationToken.None);

            Assert.True(File.Exists(finalizedPath));
            Assert.Equal(payload, await File.ReadAllTextAsync(finalizedPath, CancellationToken.None));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task DispatchRejectsUnexpectedFinalizedPath()
    {
        var root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));

        try
        {
            var sink = new TradeSinkFileSimple(new TradeSinkFileSimpleOptions(root));
            var instrument = Instrument.Parse("BTC-USDT");
            var unexpectedPath = Path.Combine(root, "other", "2026-03-12.jsonl");

            Directory.CreateDirectory(Path.GetDirectoryName(unexpectedPath)!);
            await File.WriteAllTextAsync(unexpectedPath, "{}", CancellationToken.None);

            await Assert.ThrowsAsync<InvalidOperationException>(() => sink.DispatchAsync(StubExchange, instrument, new DateOnly(2026, 3, 12), unexpectedPath, CancellationToken.None).AsTask());
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task DispatchRejectsScratchPath()
    {
        var root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));

        try
        {
            var sink = new TradeSinkFileSimple(new TradeSinkFileSimpleOptions(root));
            var instrument = Instrument.Parse("BTC-USDT");
            var scratchPath = TradeLocalDailyFilePath.BuildScratch(root, StubExchange, instrument);

            Directory.CreateDirectory(Path.GetDirectoryName(scratchPath)!);
            await File.WriteAllTextAsync(scratchPath, "{}", CancellationToken.None);

            await Assert.ThrowsAsync<InvalidOperationException>(() => sink.DispatchAsync(StubExchange, instrument, new DateOnly(2026, 3, 12), scratchPath, CancellationToken.None).AsTask());
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public async Task SnapshotDispatchAcceptsTimestampedSnapshotPathWithoutDeletingIt()
    {
        var root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));

        try
        {
            var sink = new TradeSinkFileSimple(new TradeSinkFileSimpleOptions(root));
            var instrument = Instrument.Parse("BTC-USDT");
            var snapshotPath = TradeLocalDailyFilePath.BuildSnapshot(root, StubExchange, instrument, new DateTimeOffset(2026, 3, 12, 14, 15, 16, 789, TimeSpan.Zero));
            var payload = TradeJsonlFile.BuildPayload([CreateTrade("123", new DateOnly(2026, 3, 12))]);

            Directory.CreateDirectory(Path.GetDirectoryName(snapshotPath)!);
            await File.WriteAllTextAsync(snapshotPath, payload, CancellationToken.None);

            await ((ITradeSnapshotFileDispatcher)sink).DispatchAsync(StubExchange, instrument, snapshotPath, CancellationToken.None);

            Assert.True(File.Exists(snapshotPath));
            Assert.Equal(payload, await File.ReadAllTextAsync(snapshotPath, CancellationToken.None));
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public void TradeSinkFileSimpleDoesNotAcceptTradeBatchInputs()
    {
        var acceptsTradeBatch = typeof(TradeSinkFileSimple)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .SelectMany(method => method.GetParameters())
            .Any(parameter => parameter.ParameterType == typeof(IReadOnlyList<TradeInfo>));

        Assert.False(acceptsTradeBatch);
        Assert.True(typeof(ITradeFinalizedFileDispatcher).IsAssignableFrom(typeof(TradeSinkFileSimple)));
        Assert.True(typeof(ITradeSnapshotFileDispatcher).IsAssignableFrom(typeof(TradeSinkFileSimple)));
    }

    private static TradeInfo CreateTrade(string tradeId, DateOnly utcDate)
    {
        var key = new TradeKey(StubExchange, Instrument.Parse("BTC-USDT"), tradeId);
        var timestamp = new DateTimeOffset(utcDate.Year, utcDate.Month, utcDate.Day, 13, 37, 0, TimeSpan.Zero);
        return new TradeInfo(key, timestamp, price: 10m, quantity: 0.5m);
    }
}
