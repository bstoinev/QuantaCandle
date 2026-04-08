using QuantaCandle.Infra.Generation;

namespace QuantaCandle.Infra.Tests.Generation;

/// <summary>
/// Verifies the positional command surface exposed by the CLI executable.
/// </summary>
public sealed class CandleGeneratorCommandTests
{
    [Fact]
    public void ParsesCandlizeCommandWithReadableOptions()
    {
        var options = CandleGeneratorCommand.Parse(
        [
            "candlize",
            "btc-usdt",
            "--exchange", "Binance",
            "--workDir", "W:\\QuantaCandle",
            "--dates", "20260330,20260401",
        ]);

        Assert.Equal(CandleGeneratorMode.Candlize, options.Mode);
        Assert.Equal("Binance", options.Exchange);
        Assert.Equal("BTC-USDT", options.Instrument);
        Assert.Equal("W:\\QuantaCandle", options.WorkDirectory);
        Assert.Equal([new DateOnly(2026, 3, 30), new DateOnly(2026, 4, 1)], options.Dates);
    }

    [Fact]
    public void DefaultsExchangeAndWorkDirectoryWhenOptionsAreOmitted()
    {
        var options = CandleGeneratorCommand.Parse(["scan", "btc-usdt"]);

        Assert.Equal(CandleGeneratorMode.Scan, options.Mode);
        Assert.Equal("Binance", options.Exchange);
        Assert.Equal(Directory.GetCurrentDirectory(), options.WorkDirectory);
        Assert.Equal("BTC-USDT", options.Instrument);
        Assert.Empty(options.Dates);
    }

    [Fact]
    public void ParsesAliasOptions()
    {
        var options = CandleGeneratorCommand.Parse(["heal", "btc-usdt", "-on", "20260328", "-x", "Binance", "-dir", "W:\\QuantaCandle"]);

        Assert.Equal(CandleGeneratorMode.Heal, options.Mode);
        Assert.Equal("Binance", options.Exchange);
        Assert.Equal("W:\\QuantaCandle", options.WorkDirectory);
        Assert.Equal([new DateOnly(2026, 3, 28)], options.Dates);
    }

    [Fact]
    public void RejectsMissingCommand()
    {
        var exception = Assert.Throws<ArgumentException>(() => CandleGeneratorCommand.Parse([]));

        Assert.Contains("command argument is required", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RejectsUnknownCommand()
    {
        var exception = Assert.Throws<ArgumentException>(() => CandleGeneratorCommand.Parse(["generate-candles", "btc-usdt"]));

        Assert.Contains("Unknown command 'generate-candles'", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsLegacyCommandAlias()
    {
        var exception = Assert.Throws<ArgumentException>(() => CandleGeneratorCommand.Parse(["heal-gaps", "btc-usdt"]));

        Assert.Contains("Unknown command 'heal-gaps'", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsMissingInstrument()
    {
        var exception = Assert.Throws<ArgumentException>(() => CandleGeneratorCommand.Parse(["heal"]));

        Assert.Contains("instrument argument is required", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RejectsMissingOptionValue()
    {
        var exception = Assert.Throws<ArgumentException>(() => CandleGeneratorCommand.Parse(["heal", "btc-usdt", "--dates"]));

        Assert.Contains("Option '--dates' requires a value.", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("--mode")]
    [InlineData("--source")]
    [InlineData("--date")]
    [InlineData("--inDir")]
    [InlineData("--outDir")]
    [InlineData("--instrument")]
    public void RejectsLegacyOptions(string option)
    {
        var exception = Assert.Throws<ArgumentException>(() => CandleGeneratorCommand.Parse(["heal", "btc-usdt", option, "value"]));

        Assert.Contains($"Legacy option '{option}'", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsInvalidDateFormat()
    {
        var exception = Assert.Throws<ArgumentException>(() => CandleGeneratorCommand.Parse(["heal", "btc-usdt", "--dates", "2026/03/12"]));

        Assert.Contains("yyyy-MM-dd", exception.Message, StringComparison.Ordinal);
    }
}
