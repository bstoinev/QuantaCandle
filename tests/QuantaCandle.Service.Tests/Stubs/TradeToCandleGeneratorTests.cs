using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using QuantaCandle.Service.Stubs;

namespace QuantaCandle.Service.Tests.Stubs;

public sealed class TradeToCandleGeneratorTests
{
    [Fact]
    public async Task Aggregates_basic_1m_candles()
    {
        string root = CreateTempRoot();
        try
        {
            string inputDirectory = Path.Combine(root, "input");
            string outputDirectory = Path.Combine(root, "output");

            await WriteTradeFileAsync(inputDirectory, "BTC-USDT", "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "2", "2026-03-12T12:00:20Z", 101m, 0.2m),
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.1m),
                Trade("binance", "BTC-USDT", "3", "2026-03-12T12:01:10Z", 99m, 0.3m));

            TradeToCandleGenerator generator = new TradeToCandleGenerator();
            CandleGenerationResult result = await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(inputDirectory, outputDirectory, "binance", "1m"),
                CancellationToken.None);

            Assert.Equal(3, result.InputTradeCount);
            Assert.Equal(3, result.UniqueTradeCount);
            Assert.Equal(2, result.CandleCount);

            JsonElement[] candles = await ReadCandleFileAsync(outputDirectory, "BTC-USDT", "2026-03-12");
            Assert.Equal(2, candles.Length);

            Assert.Equal(DateTimeOffset.Parse("2026-03-12T12:00:00+00:00"), candles[0].GetProperty("openTime").GetDateTimeOffset());
            Assert.Equal(100m, candles[0].GetProperty("open").GetDecimal());
            Assert.Equal(101m, candles[0].GetProperty("high").GetDecimal());
            Assert.Equal(100m, candles[0].GetProperty("low").GetDecimal());
            Assert.Equal(101m, candles[0].GetProperty("close").GetDecimal());
            Assert.Equal(0.3m, candles[0].GetProperty("volume").GetDecimal());
            Assert.Equal(2, candles[0].GetProperty("tradeCount").GetInt32());

            Assert.Equal(DateTimeOffset.Parse("2026-03-12T12:01:00+00:00"), candles[1].GetProperty("openTime").GetDateTimeOffset());
            Assert.Equal(99m, candles[1].GetProperty("open").GetDecimal());
            Assert.Equal(99m, candles[1].GetProperty("high").GetDecimal());
            Assert.Equal(99m, candles[1].GetProperty("low").GetDecimal());
            Assert.Equal(99m, candles[1].GetProperty("close").GetDecimal());
            Assert.Equal(0.3m, candles[1].GetProperty("volume").GetDecimal());
            Assert.Equal(1, candles[1].GetProperty("tradeCount").GetInt32());
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task Produces_identical_output_for_shuffled_input_order()
    {
        string root = CreateTempRoot();
        try
        {
            string inputA = Path.Combine(root, "input-a");
            string inputB = Path.Combine(root, "input-b");
            string outputA = Path.Combine(root, "output-a");
            string outputB = Path.Combine(root, "output-b");

            await WriteTradeFileAsync(inputA, "BTC-USDT", "b.jsonl",
                Trade("binance", "BTC-USDT", "2", "2026-03-12T12:00:20Z", 101m, 0.2m));
            await WriteTradeFileAsync(inputA, "BTC-USDT", "a.jsonl",
                Trade("binance", "BTC-USDT", "3", "2026-03-12T12:01:10Z", 99m, 0.3m),
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.1m));

            await WriteTradeFileAsync(inputB, "BTC-USDT", "a.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.1m));
            await WriteTradeFileAsync(inputB, "BTC-USDT", "b.jsonl",
                Trade("binance", "BTC-USDT", "2", "2026-03-12T12:00:20Z", 101m, 0.2m),
                Trade("binance", "BTC-USDT", "3", "2026-03-12T12:01:10Z", 99m, 0.3m));

            TradeToCandleGenerator generator = new TradeToCandleGenerator();
            await generator.GenerateAsync(new TradeToCandleGeneratorOptions(inputA, outputA, "binance", "1m"), CancellationToken.None);
            await generator.GenerateAsync(new TradeToCandleGeneratorOptions(inputB, outputB, "binance", "1m"), CancellationToken.None);

            IReadOnlyList<string> snapshotA = await SnapshotOutputAsync(outputA);
            IReadOnlyList<string> snapshotB = await SnapshotOutputAsync(outputB);
            Assert.Equal(snapshotA, snapshotB);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task Drops_duplicate_trades_by_trade_key()
    {
        string root = CreateTempRoot();
        try
        {
            string inputDirectory = Path.Combine(root, "input");
            string outputDirectory = Path.Combine(root, "output");

            await WriteTradeFileAsync(inputDirectory, "BTC-USDT", "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.5m),
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.5m),
                Trade("binance", "BTC-USDT", "2", "2026-03-12T12:00:40Z", 101m, 0.25m));

            TradeToCandleGenerator generator = new TradeToCandleGenerator();
            CandleGenerationResult result = await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(inputDirectory, outputDirectory, "binance", "1m"),
                CancellationToken.None);

            Assert.Equal(3, result.InputTradeCount);
            Assert.Equal(2, result.UniqueTradeCount);
            Assert.Equal(1, result.DuplicatesDropped);

            JsonElement[] candles = await ReadCandleFileAsync(outputDirectory, "BTC-USDT", "2026-03-12");
            Assert.Single(candles);
            Assert.Equal(0.75m, candles[0].GetProperty("volume").GetDecimal());
            Assert.Equal(2, candles[0].GetProperty("tradeCount").GetInt32());
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task Emits_gap_candles_with_null_ohlc_and_zero_volume()
    {
        string root = CreateTempRoot();
        try
        {
            string inputDirectory = Path.Combine(root, "input");
            string outputDirectory = Path.Combine(root, "output");

            await WriteTradeFileAsync(inputDirectory, "BTC-USDT", "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-12T00:00:10Z", 100m, 1.0m),
                Trade("binance", "BTC-USDT", "2", "2026-03-12T00:02:15Z", 102m, 0.5m));

            TradeToCandleGenerator generator = new TradeToCandleGenerator();
            CandleGenerationResult result = await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(inputDirectory, outputDirectory, "binance", "1m"),
                CancellationToken.None);

            Assert.Equal(3, result.CandleCount);

            JsonElement[] candles = await ReadCandleFileAsync(outputDirectory, "BTC-USDT", "2026-03-12");
            Assert.Equal(3, candles.Length);

            JsonElement gap = candles[1];
            Assert.Equal(DateTimeOffset.Parse("2026-03-12T00:01:00+00:00"), gap.GetProperty("openTime").GetDateTimeOffset());
            Assert.Equal(JsonValueKind.Null, gap.GetProperty("open").ValueKind);
            Assert.Equal(JsonValueKind.Null, gap.GetProperty("high").ValueKind);
            Assert.Equal(JsonValueKind.Null, gap.GetProperty("low").ValueKind);
            Assert.Equal(JsonValueKind.Null, gap.GetProperty("close").ValueKind);
            Assert.Equal(0m, gap.GetProperty("volume").GetDecimal());
            Assert.Equal(0, gap.GetProperty("tradeCount").GetInt32());
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    private static string CreateTempRoot()
    {
        string root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Service.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        return root;
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
        return new
        {
            exchange,
            instrument,
            tradeId,
            timestamp = DateTimeOffset.Parse(timestamp),
            price,
            quantity,
        };
    }

    private static async Task WriteTradeFileAsync(string inputDirectory, string instrument, string fileName, params object[] trades)
    {
        string directory = Path.Combine(inputDirectory, instrument);
        Directory.CreateDirectory(directory);

        string path = Path.Combine(directory, fileName);
        string[] lines = trades.Select(trade => JsonSerializer.Serialize(trade)).ToArray();
        string payload = string.Join(Environment.NewLine, lines) + Environment.NewLine;
        await File.WriteAllTextAsync(path, payload, CancellationToken.None);
    }

    private static async Task<JsonElement[]> ReadCandleFileAsync(string outputDirectory, string instrument, string day)
    {
        string path = Path.Combine(outputDirectory, "binance", "1m", instrument, $"{day}.jsonl");
        string[] lines = await File.ReadAllLinesAsync(path, CancellationToken.None);

        List<JsonElement> values = new List<JsonElement>(lines.Length);
        foreach (string line in lines)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            using JsonDocument doc = JsonDocument.Parse(line);
            values.Add(doc.RootElement.Clone());
        }

        return values.ToArray();
    }

    private static async Task<IReadOnlyList<string>> SnapshotOutputAsync(string outputDirectory)
    {
        string root = Path.Combine(outputDirectory, "binance", "1m");
        if (!Directory.Exists(root))
        {
            return Array.Empty<string>();
        }

        string[] files = Directory.GetFiles(root, "*.jsonl", SearchOption.AllDirectories);
        Array.Sort(files, StringComparer.Ordinal);

        List<string> snapshot = new List<string>(files.Length);
        foreach (string file in files)
        {
            string relativePath = Path.GetRelativePath(root, file).Replace('\\', '/');
            string content = await File.ReadAllTextAsync(file, CancellationToken.None);
            snapshot.Add($"{relativePath}\n{content}");
        }

        return snapshot;
    }
}
