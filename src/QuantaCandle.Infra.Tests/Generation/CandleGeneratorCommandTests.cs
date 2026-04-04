using QuantaCandle.Infra.Generation;

namespace QuantaCandle.Infra.Tests.Generation;

/// <summary>
/// Verifies the option-only command surface exposed by the candle generator executable.
/// </summary>
public sealed class CandleGeneratorCommandTests
{
    [Fact]
    public void ParsesGeneratorOptionsWithoutCommandToken()
    {
        var options = CandleGeneratorCommand.Parse(
        [
            "--mode", "generate-candles",
            "--source", "binance",
            "--timeframe", "1m",
            "--format", "jsonl",
            "--inDir", "trades-in",
            "--outDir", "candles-out",
        ]);

        Assert.Equal(CandleGeneratorMode.GenerateCandles, options.Mode);
        Assert.Equal("binance", options.Source);
        Assert.Equal("1m", options.Timeframe);
        Assert.Equal("jsonl", options.Format);
        Assert.Equal("trades-in", options.InputDirectory);
        Assert.Equal("candles-out", options.OutputDirectory);
    }

    [Fact]
    public void DefaultsToGenerateCandlesModeWhenModeIsOmitted()
    {
        var options = CandleGeneratorCommand.Parse(["--source", "binance"]);

        Assert.Equal(CandleGeneratorMode.GenerateCandles, options.Mode);
    }

    [Fact]
    public void ParsesScanGapMode()
    {
        var options = CandleGeneratorCommand.Parse(["--mode", "scan-gaps", "--inDir", "trades-in"]);

        Assert.Equal(CandleGeneratorMode.ScanGaps, options.Mode);
        Assert.Equal("trades-in", options.InputDirectory);
        Assert.Empty(options.ScanDates);
    }

    [Fact]
    public void ParsesSingleScanDateUsingCompactFormat()
    {
        var options = CandleGeneratorCommand.Parse(["--mode", "scan-gaps", "--inDir", "trades-in", "--date", "20260312"]);

        Assert.Equal([new DateOnly(2026, 3, 12)], options.ScanDates);
    }

    [Fact]
    public void ParsesMultipleScanDatesUsingMixedFormats()
    {
        var options = CandleGeneratorCommand.Parse(["--mode", "scan-gaps", "--inDir", "trades-in", "--dates", "2026-03-13,20260312"]);

        Assert.Equal(
            [
                new DateOnly(2026, 3, 12),
                new DateOnly(2026, 3, 13),
            ],
            options.ScanDates);
    }

    [Fact]
    public void RejectsInvalidScanDate()
    {
        var exception = Assert.Throws<ArgumentException>(() => CandleGeneratorCommand.Parse(["--mode", "scan-gaps", "--inDir", "trades-in", "--date", "2026/03/12"]));

        Assert.Contains("yyyy-MM-dd", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ScanGapModeRequiresInputDirectory()
    {
        var exception = Assert.Throws<ArgumentException>(() => CandleGeneratorCommand.Parse(["--mode", "scan-gaps"]));

        Assert.Contains("--inDir", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsLegacyCommandToken()
    {
        var exception = Assert.Throws<ArgumentException>(() => CandleGeneratorCommand.Parse(["generate-candles", "--source", "binance"]));

        Assert.Contains("Unexpected argument 'generate-candles'", exception.Message, StringComparison.Ordinal);
    }
}
