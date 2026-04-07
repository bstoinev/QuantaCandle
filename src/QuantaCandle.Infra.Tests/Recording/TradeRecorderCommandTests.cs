using QuantaCandle.Infra;

namespace QuantaCandle.Infra.Tests.Recording;

/// <summary>
/// Verifies the option-only command surface exposed by the trade recorder executable.
/// </summary>
public sealed class TradeRecorderCommandTests
{
    [Fact]
    public void ParsesRecorderOptionsWithoutDuration()
    {
        var options = TradeRecorderCommand.Parse(
        [
            "--source", "binance",
            "--instrument", "BTCUSDT",
            "--sink", "file",
            "--outDir", "trades-out",
        ]);

        Assert.Null(options.Duration);
        Assert.Equal("trades-out", options.SinkRegistration.FileOptions!.OutputDirectory);
        Assert.Null(options.SourceRegistration.StubOptions);
        Assert.NotNull(options.SourceRegistration.BinanceOptions);
    }

    [Fact]
    public void ParsesRecorderOptionsWithDuration()
    {
        var options = TradeRecorderCommand.Parse(
        [
            "--source", "stub",
            "--instrument", "BTCUSDT",
            "--duration", "30s",
        ]);

        Assert.Equal(TimeSpan.FromSeconds(30), options.Duration);
        Assert.NotNull(options.SourceRegistration.StubOptions);
    }

    [Fact]
    public void ParsesCheckpointIntervalOption()
    {
        var options = TradeRecorderCommand.Parse(
        [
            "--source", "stub",
            "--instrument", "BTCUSDT",
            "--checkpointInterval", "15m",
        ]);

        Assert.Equal(TimeSpan.FromMinutes(15), options.CollectorOptions.CheckpointInterval);
    }

    [Fact]
    public void DefaultsCheckpointIntervalToOneHour()
    {
        var options = TradeRecorderCommand.Parse(
        [
            "--source", "stub",
            "--instrument", "BTCUSDT",
        ]);

        Assert.Equal(TimeSpan.FromHours(1), options.CollectorOptions.CheckpointInterval);
    }

    [Fact]
    public void ParsesS3RecorderOptionsWithDeterministicLocalRootAndCheckpointInterval()
    {
        var options = TradeRecorderCommand.Parse(
        [
            "--source", "stub",
            "--instrument", "BTCUSDT",
            "--sink", "s3",
            "--outDir", "local-trades",
            "--s3Bucket", "bucket-name",
        ]);

        Assert.NotNull(options.SinkRegistration.S3Options);
        Assert.Equal("local-trades", options.SinkRegistration.S3Options.LocalRootDirectory);
        Assert.Equal(TimeSpan.FromHours(1), options.SinkRegistration.S3Options.CheckpointInterval);
        Assert.Equal(TimeSpan.FromHours(1), options.CollectorOptions.CheckpointInterval);
    }

    [Fact]
    public void RejectsMissingRequiredInstrumentWhenDurationIsOmitted()
    {
        var exception = Assert.Throws<ArgumentException>(() => TradeRecorderCommand.Parse(["--source", "binance"]));

        Assert.Contains("The --instrument option is required.", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsMissingRequiredSourceWhenDurationIsProvided()
    {
        var exception = Assert.Throws<ArgumentException>(() => TradeRecorderCommand.Parse(["--instrument", "BTCUSDT", "--duration", "30s"]));

        Assert.Contains("The --source option is required.", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void HelpMarksDurationAsOptionalAndDocumentsLongRunningMode()
    {
        using var writer = new StringWriter();

        TradeRecorderCommand.WriteHelp(writer);

        var help = writer.ToString();

        Assert.Contains("[--duration 10m]", help, StringComparison.Ordinal);
        Assert.Contains("[--checkpointInterval 1h]", help, StringComparison.Ordinal);
        Assert.Contains("Omit --duration to keep recording until the host or process is stopped.", help, StringComparison.Ordinal);
    }
}
