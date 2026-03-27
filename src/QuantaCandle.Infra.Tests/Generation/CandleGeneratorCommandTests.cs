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
            "--source", "binance",
            "--timeframe", "1m",
            "--format", "jsonl",
            "--inDir", "trades-in",
            "--outDir", "candles-out",
        ]);

        Assert.Equal("binance", options.Source);
        Assert.Equal("1m", options.Timeframe);
        Assert.Equal("jsonl", options.Format);
        Assert.Equal("trades-in", options.InputDirectory);
        Assert.Equal("candles-out", options.OutputDirectory);
    }

    [Fact]
    public void RejectsLegacyCommandToken()
    {
        var exception = Assert.Throws<ArgumentException>(() => CandleGeneratorCommand.Parse(["generate-candles", "--source", "binance"]));

        Assert.Contains("Unexpected argument 'generate-candles'", exception.Message, StringComparison.Ordinal);
    }
}
