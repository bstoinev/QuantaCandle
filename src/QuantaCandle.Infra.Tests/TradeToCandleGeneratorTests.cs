using System.Globalization;
using System.Text.Json;

using QuantaCandle.Infra.Generation;

namespace QuantaCandle.Infra.Tests;

public sealed class TradeToCandleGeneratorTests
{
    [Fact]
    public async Task Defaults_to_csv_output_when_format_is_omitted()
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

            Assert.Equal(2, result.CandleCount);

            string csvPath = Path.Combine(outputDirectory, "binance", "1m", "BTC-USDT", "2026-03-12.csv");
            string jsonlPath = Path.Combine(outputDirectory, "binance", "1m", "BTC-USDT", "2026-03-12.jsonl");
            Assert.True(File.Exists(csvPath));
            Assert.False(File.Exists(jsonlPath));

            string[][] rows = await ReadCsvRowsAsync(csvPath);
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
    public async Task Supports_explicit_csv_output()
    {
        string root = CreateTempRoot();
        try
        {
            string inputDirectory = Path.Combine(root, "input");
            string outputDirectory = Path.Combine(root, "output");

            await WriteTradeFileAsync(inputDirectory, "BTC-USDT", "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.1m));

            TradeToCandleGenerator generator = new TradeToCandleGenerator();
            await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(inputDirectory, outputDirectory, "binance", "1m", "csv"),
                CancellationToken.None);

            string csvPath = Path.Combine(outputDirectory, "binance", "1m", "BTC-USDT", "2026-03-12.csv");
            Assert.True(File.Exists(csvPath));
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    [Fact]
    public async Task Supports_explicit_jsonl_output()
    {
        string root = CreateTempRoot();
        try
        {
            string inputDirectory = Path.Combine(root, "input");
            string outputDirectory = Path.Combine(root, "output");

            await WriteTradeFileAsync(inputDirectory, "BTC-USDT", "2026-03-12.jsonl",
                Trade("binance", "BTC-USDT", "2", "2026-03-12T12:00:20Z", 101m, 0.2m),
                Trade("binance", "BTC-USDT", "1", "2026-03-12T12:00:05Z", 100m, 0.1m));

            TradeToCandleGenerator generator = new TradeToCandleGenerator();
            await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(inputDirectory, outputDirectory, "binance", "1m", "jsonl"),
                CancellationToken.None);

            string jsonlPath = Path.Combine(outputDirectory, "binance", "1m", "BTC-USDT", "2026-03-12.jsonl");
            string csvPath = Path.Combine(outputDirectory, "binance", "1m", "BTC-USDT", "2026-03-12.csv");

            Assert.True(File.Exists(jsonlPath));
            Assert.False(File.Exists(csvPath));

            JsonElement[] candles = await ReadJsonlFileAsync(jsonlPath);
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
    public async Task Emits_gap_candles_as_empty_ohlc_in_csv()
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
            await generator.GenerateAsync(
                new TradeToCandleGeneratorOptions(inputDirectory, outputDirectory, "binance", "1m", "csv"),
                CancellationToken.None);

            string csvPath = Path.Combine(outputDirectory, "binance", "1m", "BTC-USDT", "2026-03-12.csv");
            string[][] rows = await ReadCsvRowsAsync(csvPath);

            Assert.Equal(4, rows.Length);

            string[] gap = rows[2];
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

            IReadOnlyList<string> snapshotA = await SnapshotOutputAsync(outputA, "*.csv");
            IReadOnlyList<string> snapshotB = await SnapshotOutputAsync(outputB, "*.csv");
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

            string csvPath = Path.Combine(outputDirectory, "binance", "1m", "BTC-USDT", "2026-03-12.csv");
            string[][] rows = await ReadCsvRowsAsync(csvPath);
            Assert.Equal("0.75", rows[1][6]);
            Assert.Equal("2", rows[1][7]);
        }
        finally
        {
            DeleteDirectoryIfExists(root);
        }
    }

    private static string CreateTempRoot()
    {
        string root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
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
            timestamp = DateTimeOffset.Parse(timestamp, CultureInfo.InvariantCulture),
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

    private static async Task<string[][]> ReadCsvRowsAsync(string path)
    {
        string[] lines = await File.ReadAllLinesAsync(path, CancellationToken.None);
        return lines
            .Where(line => !string.IsNullOrWhiteSpace(line))
            .Select(line => line.Split(','))
            .ToArray();
    }

    private static async Task<JsonElement[]> ReadJsonlFileAsync(string path)
    {
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

    private static async Task<IReadOnlyList<string>> SnapshotOutputAsync(string outputDirectory, string pattern)
    {
        string root = Path.Combine(outputDirectory, "binance", "1m");
        if (!Directory.Exists(root))
        {
            return Array.Empty<string>();
        }

        string[] files = Directory.GetFiles(root, pattern, SearchOption.AllDirectories);
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
