using QuantaCandle.Infra;

namespace QuantaCandle.Infra.Tests.Recording;

/// <summary>
/// Verifies the qc-style recorder command surface exposed by the trade recorder executable.
/// </summary>
public sealed class TradeRecorderCommandTests
{
    [Fact]
    public void BindsFirstPositionalArgumentToInstrument()
    {
        var options = TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            "--exchange", "binance",
        ]);

        Assert.Equal(["BTC-USDT"], options.CollectorOptions.Instruments);
        Assert.NotNull(options.SourceRegistration.BinanceOptions);
        Assert.Null(options.SourceRegistration.StubOptions);
    }

    [Theory]
    [InlineData("--exchange")]
    [InlineData("-x")]
    public void BindsExchangeOptionAndAlias(string optionName)
    {
        var options = TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            optionName, "stub",
        ]);

        Assert.NotNull(options.SourceRegistration.StubOptions);
        Assert.Null(options.SourceRegistration.BinanceOptions);
        Assert.Equal("Stub", options.SourceRegistration.StubOptions.Exchange.Value);
    }

    [Theory]
    [InlineData("--sink")]
    [InlineData("-to")]
    public void BindsSinkOptionAndAlias(string optionName)
    {
        var options = TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            "--exchange", "stub",
            optionName, "null",
        ]);

        Assert.Null(options.SinkRegistration.FileOptions);
        Assert.Null(options.SinkRegistration.S3Options);
    }

    [Fact]
    public void BindsS3Options()
    {
        var options = TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            "--exchange", "stub",
            "--sink", "s3",
            "--s3Bucket", "bucket-name",
            "--s3Prefix", "trade-data/",
            "--outDir", "local-trades",
        ]);

        Assert.NotNull(options.SinkRegistration.S3Options);
        Assert.Equal("bucket-name", options.SinkRegistration.S3Options.BucketName);
        Assert.Equal("trade-data/", options.SinkRegistration.S3Options.Prefix);
        Assert.Equal("local-trades", options.SinkRegistration.S3Options.LocalRootDirectory);
    }

    [Fact]
    public void ParsesRealisticRecorderCommandLine()
    {
        var options = TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            "--exchange", "Binance",
            "--sink", "s3",
            "--s3Bucket", "quanta-candle-bucket-eu-north-1",
            "--s3Prefix", "trade-data/",
            "--duration", "30s",
            "--rate", "25",
            "--capacity", "2048",
            "--batchSize", "250",
            "--flushInterval", "2s",
            "--checkpointInterval", "30m",
            "--cacheSize", "4096",
            "--outDir", "trade-data",
        ]);

        Assert.Equal(TimeSpan.FromSeconds(30), options.Duration);
        Assert.Equal(4096, options.CacheSize);
        Assert.Equal(["BTC-USDT"], options.CollectorOptions.Instruments);
        Assert.Equal(25, options.CollectorOptions.MaxTradesPerSecond);
        Assert.Equal(2048, options.CollectorOptions.ChannelCapacity);
        Assert.Equal(250, options.CollectorOptions.BatchSize);
        Assert.Equal(TimeSpan.FromSeconds(2), options.CollectorOptions.FlushInterval);
        Assert.Equal(TimeSpan.FromMinutes(30), options.CollectorOptions.CheckpointInterval);
        Assert.NotNull(options.SourceRegistration.BinanceOptions);
        Assert.NotNull(options.SinkRegistration.S3Options);
        Assert.Equal("quanta-candle-bucket-eu-north-1", options.SinkRegistration.S3Options.BucketName);
        Assert.Equal("trade-data/", options.SinkRegistration.S3Options.Prefix);
    }

    [Fact]
    public void DefaultsSinkToFileWhenSinkOptionIsOmitted()
    {
        var options = TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            "--exchange", "stub",
        ]);

        Assert.NotNull(options.SinkRegistration.FileOptions);
        Assert.Equal("trade-data", options.SinkRegistration.FileOptions.OutputDirectory);
        Assert.Null(options.SinkRegistration.S3Options);
    }

    [Fact]
    public void DefaultsCheckpointIntervalToOneHour()
    {
        var options = TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            "--exchange", "stub",
        ]);

        Assert.Equal(TimeSpan.FromHours(1), options.CollectorOptions.CheckpointInterval);
    }

    [Fact]
    public void DefaultsCacheSizeToOneThousandTwentyFour()
    {
        var options = TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            "--exchange", "stub",
        ]);

        Assert.Equal(1024, options.CacheSize);
    }

    [Fact]
    public void ParsesCacheSizeOption()
    {
        var options = TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            "--exchange", "stub",
            "--cacheSize", "2048",
        ]);

        Assert.Equal(2048, options.CacheSize);
    }

    [Fact]
    public void RejectsZeroCacheSize()
    {
        var exception = Assert.Throws<ArgumentException>(() => TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            "--exchange", "stub",
            "--cacheSize", "0",
        ]));

        Assert.Contains("The --cacheSize option must be greater than zero.", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsNegativeCacheSize()
    {
        var exception = Assert.Throws<ArgumentException>(() => TradeRecorderCommand.Parse(
        [
            "BTCUSDT",
            "--exchange", "stub",
            "--cacheSize", "-1",
        ]));

        Assert.Contains("The --cacheSize option must be greater than zero.", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsMissingPositionalInstrument()
    {
        var exception = Assert.Throws<ArgumentException>(() => TradeRecorderCommand.Parse(["--exchange", "binance"]));

        Assert.Contains("first positional argument", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsMissingRequiredExchange()
    {
        var exception = Assert.Throws<ArgumentException>(() => TradeRecorderCommand.Parse(["BTCUSDT", "--duration", "30s"]));

        Assert.Contains("The --exchange option is required.", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("--instrument")]
    [InlineData("--source")]
    public void RejectsLegacyOptions(string optionName)
    {
        var exception = Assert.Throws<ArgumentException>(() => TradeRecorderCommand.Parse(["BTCUSDT", optionName, "value", "--exchange", "stub"]));

        Assert.Contains($"Legacy option '{optionName}'", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void HelpDocumentsPositionalInstrumentAndSinkAlias()
    {
        using var writer = new StringWriter();

        TradeRecorderCommand.WriteHelp(writer);

        var help = writer.ToString();

        Assert.Contains("BTCUSDT --exchange Binance|-x Binance", help, StringComparison.Ordinal);
        Assert.Contains("[--sink file|s3|null|-to file|s3|null]", help, StringComparison.Ordinal);
        Assert.Contains("Omit --duration to keep recording until the host or process is stopped.", help, StringComparison.Ordinal);
        Assert.Contains("[--outDir trade-data]", help, StringComparison.Ordinal);
        Assert.Contains("<working-dir>\\trade-data\\<exchange>\\<instrument>\\yyyy-MM-dd.jsonl", help, StringComparison.Ordinal);
    }
}
