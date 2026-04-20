using QuantaCandle.Core.Trading;
using QuantaCandle.Infra;

namespace QuantaCandle.CLI.Tests;

/// <summary>
/// Verifies inclusive candlize UTC date-range resolution from local daily trade files.
/// </summary>
public sealed class CandlizeDateRangeResolverTests
{
    [Fact]
    public void ResolvesBothOmittedToMinAndMaxFileDates()
    {
        var root = CreateTempRoot();

        try
        {
            WriteDailyTradeFile(root, "Binance", "BTC-USDT", new DateOnly(2026, 3, 30));
            WriteDailyTradeFile(root, "Binance", "BTC-USDT", new DateOnly(2026, 4, 1));
            WriteNonDateFile(root, "Binance", "BTC-USDT", "notes.jsonl");

            var result = CandlizeDateRangeResolver.Resolve(
                CliPathRootResolver.GetTradeDataRoot(root),
                "Binance",
                "BTC-USDT",
                null,
                null);

            Assert.Equal(new DateOnly(2026, 3, 30), result.BeginDateUtc);
            Assert.Equal(new DateOnly(2026, 4, 1), result.EndDateUtc);
            Assert.Equal(
                [
                    new DateOnly(2026, 3, 30),
                    new DateOnly(2026, 4, 1),
                ],
                result.FilesInRange.Select(static file => file.UtcDate).ToArray());
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public void ResolvesMissingBeginToMinFileDate()
    {
        var root = CreateTempRoot();

        try
        {
            WriteDailyTradeFile(root, "Binance", "BTC-USDT", new DateOnly(2026, 3, 30));
            WriteDailyTradeFile(root, "Binance", "BTC-USDT", new DateOnly(2026, 4, 1));

            var result = CandlizeDateRangeResolver.Resolve(
                CliPathRootResolver.GetTradeDataRoot(root),
                "Binance",
                "BTC-USDT",
                null,
                new DateOnly(2026, 3, 31));

            Assert.Equal(new DateOnly(2026, 3, 30), result.BeginDateUtc);
            Assert.Equal(new DateOnly(2026, 3, 31), result.EndDateUtc);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public void ResolvesMissingEndToMaxFileDate()
    {
        var root = CreateTempRoot();

        try
        {
            WriteDailyTradeFile(root, "Binance", "BTC-USDT", new DateOnly(2026, 3, 30));
            WriteDailyTradeFile(root, "Binance", "BTC-USDT", new DateOnly(2026, 4, 1));

            var result = CandlizeDateRangeResolver.Resolve(
                CliPathRootResolver.GetTradeDataRoot(root),
                "Binance",
                "BTC-USDT",
                new DateOnly(2026, 3, 31),
                null);

            Assert.Equal(new DateOnly(2026, 3, 31), result.BeginDateUtc);
            Assert.Equal(new DateOnly(2026, 4, 1), result.EndDateUtc);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public void RejectsReversedRange()
    {
        var root = CreateTempRoot();

        try
        {
            var exception = Assert.Throws<ArgumentException>(() => CandlizeDateRangeResolver.Resolve(
                CliPathRootResolver.GetTradeDataRoot(root),
                "Binance",
                "BTC-USDT",
                new DateOnly(2026, 4, 2),
                new DateOnly(2026, 4, 1)));

            Assert.Contains("begin UTC date '2026-04-02' cannot be later than end UTC date '2026-04-01'", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public void FailsWhenResolutionNeedsFilesButFolderHasNoParseableDailyFiles()
    {
        var root = CreateTempRoot();

        try
        {
            WriteNonDateFile(root, "Binance", "BTC-USDT", "qc-scratch.jsonl");
            WriteNonDateFile(root, "Binance", "BTC-USDT", "notes.jsonl");

            var exception = Assert.Throws<InvalidOperationException>(() => CandlizeDateRangeResolver.Resolve(
                CliPathRootResolver.GetTradeDataRoot(root),
                "Binance",
                "BTC-USDT",
                null,
                new DateOnly(2026, 4, 1)));

            Assert.Contains("has no parseable daily trade files", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public void IgnoresNonDateFilesWhenResolvingAvailableBounds()
    {
        var root = CreateTempRoot();

        try
        {
            WriteNonDateFile(root, "Binance", "BTC-USDT", "aaa.jsonl");
            WriteDailyTradeFile(root, "Binance", "BTC-USDT", new DateOnly(2026, 3, 30));
            WriteNonDateFile(root, "Binance", "BTC-USDT", "2026-03-30.partial.jsonl");
            WriteDailyTradeFile(root, "Binance", "BTC-USDT", new DateOnly(2026, 4, 1));

            var result = CandlizeDateRangeResolver.Resolve(
                CliPathRootResolver.GetTradeDataRoot(root),
                "Binance",
                "BTC-USDT",
                null,
                null);

            Assert.Equal(2, result.FilesInRange.Count);
            Assert.DoesNotContain(result.FilesInRange, static file => file.Path.EndsWith("partial.jsonl", StringComparison.OrdinalIgnoreCase));
            Assert.DoesNotContain(result.FilesInRange, static file => file.Path.EndsWith("aaa.jsonl", StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    private static string CreateTempRoot()
    {
        var result = Path.Combine(Path.GetTempPath(), "QuantaCandle.CLI.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(result);
        return result;
    }

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
        {
            Directory.Delete(path, recursive: true);
        }
    }

    private static void WriteDailyTradeFile(string workDirectory, string exchange, string instrument, DateOnly utcDate)
    {
        var path = TradeLocalDailyFilePath.Build(CliPathRootResolver.GetTradeDataRoot(workDirectory), new ExchangeId(exchange), Instrument.Parse(instrument), utcDate);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, string.Empty);
    }

    private static void WriteNonDateFile(string workDirectory, string exchange, string instrument, string fileName)
    {
        var directory = Path.Combine(CliPathRootResolver.GetTradeDataRoot(workDirectory), exchange, instrument);
        Directory.CreateDirectory(directory);
        File.WriteAllText(Path.Combine(directory, fileName), string.Empty);
    }
}
