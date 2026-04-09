using System.Globalization;
using System.Text.Json;

using QuantaCandle.Infra.Generation;

namespace QuantaCandle.Infra.Tests;

/// <summary>
/// Verifies in-place candle generation over the real work directory layout.
/// </summary>
public sealed class TradeToCandleGeneratorTests
{
    [Fact]
    public async Task DefaultsToCsvOutputIfFormatIsOmitted()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(root, "binance", "BTC-USDT", "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "2", "2026-03-12T12:00:20Z", 101m, 0.2m),
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.1m),
                Trade("binance", "BTC-USDT", "3", "2026-03-12T12:01:10Z", 99m, 0.3m));

            var generator = new TradeToCandleGenerator();
            var result = await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(root, "binance", "BTC-USDT", "1m", []),
                CancellationToken.None);

            Assert.Equal(2, result.CandleCount);

            var csvPath = Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-03-12.csv");
            var jsonlPath = Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-03-12.jsonl");
            Assert.True(File.Exists(csvPath));
            Assert.False(File.Exists(jsonlPath));

            var rows = await ReadCsvRowsAsync(csvPath);
            Assert.Equal("OpenTimeUtc", rows[0][0]);
            Assert.Equal("Instrument", rows[0][1]);
            Assert.Equal("Open", rows[0][2]);
            Assert.Equal("High", rows[0][3]);
            Assert.Equal("Low", rows[0][4]);
            Assert.Equal("Close", rows[0][5]);
            Assert.Equal("Volume", rows[0][6]);
            Assert.Equal("TradeCount", rows[0][7]);
            Assert.Equal(DateTimeOffset.Parse("2026-03-12T12:00:00Z"), DateTimeOffset.Parse(rows[1][0], CultureInfo.InvariantCulture));
            Assert.Equal("BTC-USDT", rows[1][1]);
            Assert.Equal("100", rows[1][2]);
            Assert.Equal("101", rows[1][3]);
            Assert.Equal("100", rows[1][4]);
            Assert.Equal("101", rows[1][5]);
            Assert.Equal("0.3", rows[1][6]);
            Assert.Equal("2", rows[1][7]);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task SupportsEplicitCsvOutput()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(root, "binance", "BTC-USDT", "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.1m));

            var generator = new TradeToCandleGenerator();
            await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(root, "binance", "BTC-USDT", "1m", [], "csv"),
                CancellationToken.None);

            var csvPath = Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-03-12.csv");
            Assert.True(File.Exists(csvPath));
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task SupportsExplicitJsonlOutput()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(root, "binance", "BTC-USDT", "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "2", "2026-03-12T12:00:20Z", 101m, 0.2m),
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.1m));

            var generator = new TradeToCandleGenerator();
            await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(root, "binance", "BTC-USDT", "1m", [], "jsonl"),
                CancellationToken.None);

            var jsonlPath = Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-03-12.jsonl");
            var csvPath = Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-03-12.csv");

            Assert.True(File.Exists(jsonlPath));
            Assert.False(File.Exists(csvPath));

            var candles = await ReadJsonlFileAsync(jsonlPath);
            Assert.Single(candles);
            Assert.Equal(100m, candles[0].GetProperty("open").GetDecimal());
            Assert.Equal(101m, candles[0].GetProperty("high").GetDecimal());
            Assert.Equal(100m, candles[0].GetProperty("low").GetDecimal());
            Assert.Equal(101m, candles[0].GetProperty("close").GetDecimal());
            Assert.Equal(0.3m, candles[0].GetProperty("volume").GetDecimal());
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task EmitsGapCandlesAsEmptyOhlcInCsv()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(root, "binance", "BTC-USDT", "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-12T00:00:10Z", 100m, 1.0m),
                Trade("binance", "BTC-USDT", "2", "2026-03-12T00:02:15Z", 102m, 0.5m));

            var generator = new TradeToCandleGenerator();
            await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(root, "binance", "BTC-USDT", "1m", [], "csv"),
                CancellationToken.None);

            var csvPath = Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-03-12.csv");
            var rows = await ReadCsvRowsAsync(csvPath);

            Assert.Equal(4, rows.Length);

            var gap = rows[2];
            Assert.Equal(DateTimeOffset.Parse("2026-03-12T00:01:00Z"), DateTimeOffset.Parse(gap[0], CultureInfo.InvariantCulture));
            Assert.Equal("BTC-USDT", gap[1]);
            Assert.Equal(string.Empty, gap[2]);
            Assert.Equal(string.Empty, gap[3]);
            Assert.Equal(string.Empty, gap[4]);
            Assert.Equal(string.Empty, gap[5]);
            Assert.Equal("0", gap[6]);
            Assert.Equal("0", gap[7]);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task ProducesIdenticalOutputForShuffledInputOrder()
    {
        var root = CreateTempRoot();

        try
        {
            var rootA = Path.Combine(root, "a");
            var rootB = Path.Combine(root, "b");

            await WriteTradeFileAsync(rootA, "binance", "BTC-USDT", "b.jsonl",
                Trade("binance", "BTC-USDT", "2", "2026-03-12T12:00:20Z", 101m, 0.2m));
            await WriteTradeFileAsync(rootA, "binance", "BTC-USDT", "a.jsonl",
                Trade("binance", "BTC-USDT", "3", "2026-03-12T12:01:10Z", 99m, 0.3m),
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.1m));

            await WriteTradeFileAsync(rootB, "binance", "BTC-USDT", "a.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.1m));
            await WriteTradeFileAsync(rootB, "binance", "BTC-USDT", "b.jsonl",
                Trade("binance", "BTC-USDT", "2", "2026-03-12T12:00:20Z", 101m, 0.2m),
                Trade("binance", "BTC-USDT", "3", "2026-03-12T12:01:10Z", 99m, 0.3m));

            var generator = new TradeToCandleGenerator();
            await generator.GenerateAsync(new TradeToCandleGeneratorOptions(rootA, "binance", "BTC-USDT", "1m", []), CancellationToken.None);
            await generator.GenerateAsync(new TradeToCandleGeneratorOptions(rootB, "binance", "BTC-USDT", "1m", []), CancellationToken.None);

            var snapshotA = await SnapshotOutputAsync(rootA, "*.csv");
            var snapshotB = await SnapshotOutputAsync(rootB, "*.csv");
            Assert.Equal(snapshotA, snapshotB);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task DropsDuplicateTradesByTradeKey()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(root, "binance", "BTC-USDT", "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.5m),
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.5m),
                Trade("binance", "BTC-USDT", "2", "2026-03-12T12:00:40Z", 101m, 0.25m));

            var generator = new TradeToCandleGenerator();
            var result = await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(root, "binance", "BTC-USDT", "1m", []),
                CancellationToken.None);

            Assert.Equal(3, result.InputTradeCount);
            Assert.Equal(2, result.UniqueTradeCount);
            Assert.Equal(1, result.DuplicatesDropped);

            var csvPath = Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-03-12.csv");
            var rows = await ReadCsvRowsAsync(csvPath);
            Assert.Equal("0.75", rows[1][6]);
            Assert.Equal("2", rows[1][7]);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task ScopeSelectionUsesOnlyRequestedInstrumentFiles()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(root, "binance", "BTC-USDT", "2026-03-30.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-30T12:00:05Z", 100m, 1m));
            await WriteTradeFileAsync(root, "binance", "ETH-USDT", "2026-03-30.jsonl",
                Trade("binance", "ETH-USDT", "1", "2026-03-30T12:00:05Z", 200m, 2m));

            var generator = new TradeToCandleGenerator();
            var result = await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(root, "binance", "BTC-USDT", "1m", []),
                CancellationToken.None);

            Assert.Equal(1, result.InputTradeCount);
            Assert.True(File.Exists(Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-03-30.csv")));
            Assert.False(File.Exists(Path.Combine(root, "candles-out", "binance", "1m", "ETH-USDT", "2026-03-30.csv")));
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task ScopeSelectionUsesOnlyRequestedDates()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(root, "binance", "BTC-USDT", "2026-03-30.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-30T12:00:05Z", 100m, 1m));
            await WriteTradeFileAsync(root, "binance", "BTC-USDT", "2026-04-01.jsonl",
                Trade("binance", "BTC-USDT", "2", "2026-04-01T12:00:05Z", 101m, 1m));
            await WriteTradeFileAsync(root, "binance", "BTC-USDT", "2026-04-02.jsonl",
                Trade("binance", "BTC-USDT", "3", "2026-04-02T12:00:05Z", 102m, 1m));

            var generator = new TradeToCandleGenerator();
            var result = await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(
                    root,
                    "binance",
                    "BTC-USDT",
                    "1m",
                    [
                        new DateOnly(2026, 3, 30),
                        new DateOnly(2026, 4, 1),
                    ]),
                CancellationToken.None);

            Assert.Equal(2, result.InputTradeCount);
            Assert.True(File.Exists(Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-03-30.csv")));
            Assert.True(File.Exists(Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-04-01.csv")));
            Assert.False(File.Exists(Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-04-02.csv")));
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task ScopedGenerationDoesNotDeleteUnrelatedExistingOutput()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(root, "binance", "BTC-USDT", "2026-03-30.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-30T12:00:05Z", 100m, 1m));

            var unrelatedInstrumentPath = Path.Combine(root, "candles-out", "binance", "1m", "ETH-USDT", "2026-03-30.csv");
            var unrelatedDatePath = Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-04-02.csv");
            Directory.CreateDirectory(Path.GetDirectoryName(unrelatedInstrumentPath)!);
            Directory.CreateDirectory(Path.GetDirectoryName(unrelatedDatePath)!);
            await File.WriteAllTextAsync(unrelatedInstrumentPath, "keep-eth" + Environment.NewLine, CancellationToken.None);
            await File.WriteAllTextAsync(unrelatedDatePath, "keep-date" + Environment.NewLine, CancellationToken.None);

            var generator = new TradeToCandleGenerator();
            await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(root, "binance", "BTC-USDT", "1m", [new DateOnly(2026, 3, 30)]),
                CancellationToken.None);

            Assert.Equal("keep-eth" + Environment.NewLine, await File.ReadAllTextAsync(unrelatedInstrumentPath, CancellationToken.None));
            Assert.Equal("keep-date" + Environment.NewLine, await File.ReadAllTextAsync(unrelatedDatePath, CancellationToken.None));
            Assert.True(File.Exists(Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-03-30.csv")));
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task UsesRequestedExchangeFolder()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(root, "binance", "BTC-USDT", "2026-03-30.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-30T12:00:05Z", 100m, 1m));
            await WriteTradeFileAsync(root, "stub", "BTC-USDT", "2026-03-30.jsonl",
                Trade("stub", "BTC-USDT", "1", "2026-03-30T12:00:05Z", 500m, 5m));

            var generator = new TradeToCandleGenerator();
            var result = await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(root, "stub", "BTC-USDT", "1m", []),
                CancellationToken.None);

            Assert.Equal(1, result.InputTradeCount);
            var csvPath = Path.Combine(root, "candles-out", "stub", "1m", "BTC-USDT", "2026-03-30.csv");
            var rows = await ReadCsvRowsAsync(csvPath);
            Assert.Equal("500", rows[1][2]);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task DoesNotReadMatchingFilesFromDifferentExchangeFolder()
    {
        var root = CreateTempRoot();

        try
        {
            await WriteTradeFileAsync(root, "binance", "BTC-USDT", "2026-03-30.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-30T12:00:05Z", 100m, 1m));
            await WriteTradeFileAsync(root, "stub", "BTC-USDT", "2026-03-30.jsonl",
                Trade("stub", "BTC-USDT", "1", "2026-03-30T12:00:05Z", 500m, 5m));

            var generator = new TradeToCandleGenerator();
            var result = await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(root, "binance", "BTC-USDT", "1m", [new DateOnly(2026, 3, 30)]),
                CancellationToken.None);

            Assert.Equal(1, result.InputTradeCount);
            var csvPath = Path.Combine(root, "candles-out", "binance", "1m", "BTC-USDT", "2026-03-30.csv");
            var rows = await ReadCsvRowsAsync(csvPath);
            Assert.Equal("100", rows[1][2]);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    private static string CreateTempRoot()
    {
        var result = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
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

    private static object Trade(string exchange, string instrument, string tradeId, string timestamp, decimal price, decimal quantity)
    {
        var result = new
        {
            exchange,
            instrument,
            tradeId,
            timestamp = DateTimeOffset.Parse(timestamp, CultureInfo.InvariantCulture),
            price,
            quantity,
        };
        return result;
    }

    private static async Task WriteTradeFileAsync(string workDirectory, string exchange, string instrument, string fileName, params object[] trades)
    {
        var directory = Path.Combine(workDirectory, "trades-out", exchange, instrument);
        Directory.CreateDirectory(directory);

        var path = Path.Combine(directory, fileName);
        var lines = trades.Select(static trade => JsonSerializer.Serialize(trade)).ToArray();
        var payload = string.Join(Environment.NewLine, lines) + Environment.NewLine;
        await File.WriteAllTextAsync(path, payload, CancellationToken.None);
    }

    private static async Task<string[][]> ReadCsvRowsAsync(string path)
    {
        var lines = await File.ReadAllLinesAsync(path, CancellationToken.None);
        var result = lines
            .Where(static line => !string.IsNullOrWhiteSpace(line))
            .Select(static line => line.Split(','))
            .ToArray();
        return result;
    }

    private static async Task<JsonElement[]> ReadJsonlFileAsync(string path)
    {
        var lines = await File.ReadAllLinesAsync(path, CancellationToken.None);
        var values = new List<JsonElement>(lines.Length);

        foreach (var line in lines)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            using var doc = JsonDocument.Parse(line);
            values.Add(doc.RootElement.Clone());
        }

        return values.ToArray();
    }

    private static async Task<IReadOnlyList<string>> SnapshotOutputAsync(string workDirectory, string pattern)
    {
        var root = Path.Combine(workDirectory, "candles-out", "binance", "1m");
        if (!Directory.Exists(root))
        {
            return Array.Empty<string>();
        }

        var files = Directory.GetFiles(root, pattern, SearchOption.AllDirectories);
        Array.Sort(files, StringComparer.Ordinal);

        var snapshot = new List<string>(files.Length);
        foreach (var file in files)
        {
            var relativePath = Path.GetRelativePath(root, file).Replace('\\', '/');
            var content = await File.ReadAllTextAsync(file, CancellationToken.None);
            snapshot.Add($"{relativePath}\n{content}");
        }

        return snapshot;
    }
}
