using System.Reflection;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Tests.Storage;

public sealed class TradeSinkFileSimpleTests
{
    [Fact]
    public async Task DispatchUsesExistingFinalizedDailyFile()
    {
        var root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));

        try
        {
            var sink = new TradeSinkFileSimple(new TradeSinkFileSimpleOptions(root));
            var instrument = Instrument.Parse("BTC-USDT");
            var utcDate = new DateOnly(2026, 3, 12);
            var finalizedPath = TradeLocalDailyFilePath.Build(root, instrument, utcDate);
            var payload = TradeJsonlFile.BuildPayload([CreateTrade("123", utcDate)]);

            Directory.CreateDirectory(Path.GetDirectoryName(finalizedPath)!);
            await File.WriteAllTextAsync(finalizedPath, payload, CancellationToken.None);

            await sink.DispatchAsync(instrument, utcDate, finalizedPath, CancellationToken.None);

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

            await Assert.ThrowsAsync<InvalidOperationException>(() => sink.DispatchAsync(instrument, new DateOnly(2026, 3, 12), unexpectedPath, CancellationToken.None).AsTask());
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
    }

    private static TradeInfo CreateTrade(string tradeId, DateOnly utcDate)
    {
        var key = new TradeKey(new ExchangeId("Stub"), Instrument.Parse("BTC-USDT"), tradeId);
        var timestamp = new DateTimeOffset(utcDate.Year, utcDate.Month, utcDate.Day, 13, 37, 0, TimeSpan.Zero);
        return new TradeInfo(key, timestamp, price: 10m, quantity: 0.5m);
    }
}
