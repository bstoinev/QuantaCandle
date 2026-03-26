using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using QuantaCandle.CLI.Commands;

namespace QuantaCandle.Infra.Tests.Integration;

public sealed class CliFlowValidationTests
{
    [Fact]
    public async Task CollectThenGenerateCreatesCandleFilesWithoutNetworkDependency()
    {
        string root = Path.Combine(Path.GetTempPath(), "QuantaCandle.Infra.Tests", Guid.NewGuid().ToString("N"));
        string tradeDirectory = Path.Combine(root, "trades");
        string candleDirectory = Path.Combine(root, "candles");

        Directory.CreateDirectory(tradeDirectory);

        try
        {
            int collectExitCode = await CollectTradesCommand.RunAsync(
            [
                "collect-trades",
                "--source", "stub",
                "--instrument", "BTCUSDT",
                "--duration", "1500ms",
                "--rate", "20",
                "--batchSize", "1",
                "--flushInterval", "100ms",
                "--sink", "file",
                "--outDir", tradeDirectory,
            ]);
            Assert.Equal(0, collectExitCode);

            string[] tradeFiles = Directory.GetFiles(tradeDirectory, "*.jsonl", SearchOption.AllDirectories);
            Assert.NotEmpty(tradeFiles);

            await RewriteExchangeToBinanceAsync(tradeFiles);

            int generateExitCode = await GenerateCandlesCommand.RunAsync(new[]
            {
                "generate-candles",
                "--source", "binance",
                "--timeframe", "1m",
                "--inDir", tradeDirectory,
                "--outDir", candleDirectory,
            });
            Assert.Equal(0, generateExitCode);

            string candleInstrumentDirectory = Path.Combine(candleDirectory, "binance", "1m", "BTC-USDT");
            string[] candleFiles = Directory.GetFiles(candleInstrumentDirectory, "*.csv", SearchOption.AllDirectories);
            Assert.NotEmpty(candleFiles);

            string firstFile = candleFiles.OrderBy(path => path, StringComparer.Ordinal).First();
            string[] lines = await File.ReadAllLinesAsync(firstFile, CancellationToken.None);

            Assert.NotEmpty(lines);
            Assert.Equal("OpenTimeUtc,Instrument,Open,High,Low,Close,Volume,TradeCount", lines[0]);
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

    private static async Task RewriteExchangeToBinanceAsync(IEnumerable<string> files)
    {
        foreach (string file in files.OrderBy(path => path, StringComparer.Ordinal))
        {
            string[] lines = await File.ReadAllLinesAsync(file, CancellationToken.None);
            List<string> rewrittenLines = new List<string>(lines.Length);

            foreach (string line in lines)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                using JsonDocument doc = JsonDocument.Parse(line);
                JsonElement root = doc.RootElement;

                string instrument = root.GetProperty("instrument").GetString() ?? string.Empty;
                string tradeId = root.GetProperty("tradeId").GetString() ?? string.Empty;
                DateTimeOffset timestamp = root.GetProperty("timestamp").GetDateTimeOffset();
                decimal price = root.GetProperty("price").GetDecimal();
                decimal quantity = root.GetProperty("quantity").GetDecimal();

                string rewritten = JsonSerializer.Serialize(new
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

            string payload = string.Join(Environment.NewLine, rewrittenLines) + Environment.NewLine;
            await File.WriteAllTextAsync(file, payload, CancellationToken.None);
        }
    }
}
