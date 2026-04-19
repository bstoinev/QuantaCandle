namespace QuantaCandle.CLI.Tests;

/// <summary>
/// Verifies CLI argument parsing, dispatch, and console flow at the application boundary.
/// </summary>
public sealed class CliApplicationTests
{
    [Fact]
    public async Task CandlizeModeDispatchesToRunnerWithParsedOptions()
    {
        var runner = new RecordingRunner
        {
            CandlizeImplementation = async (options, writer, _) =>
            {
                await writer.WriteLineAsync("Candles written: 2").ConfigureAwait(false);
                return 0;
            },
        };
        var sut = new CliApplication(runner);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        var exitCode = await sut.Run(
            ["candlize", "btc-usdt", "--timeFrame", "10s", "--workDir", "W:\\QuantaCandle", "--dates", "20260330,20260401"],
            outputWriter,
            errorWriter,
            CancellationToken.None);

        Assert.Equal(0, exitCode);
        Assert.NotNull(runner.CandlizeOptions);
        Assert.Equal(CliMode.Candlize, runner.CandlizeOptions!.Mode);
        Assert.Equal("W:\\QuantaCandle", runner.CandlizeOptions.WorkDirectory);
        Assert.Equal("Binance", runner.CandlizeOptions.Exchange);
        Assert.Equal("BTC-USDT", runner.CandlizeOptions.Instrument);
        Assert.Equal("10s", runner.CandlizeOptions.Timeframe);
        Assert.Equal(
            [
                new DateOnly(2026, 3, 30),
                new DateOnly(2026, 4, 1),
            ],
            runner.CandlizeOptions.Dates);
        Assert.Contains("Candles written: 2", outputWriter.ToString(), StringComparison.Ordinal);
        Assert.Equal(string.Empty, errorWriter.ToString());
        Assert.Equal(1, runner.CandlizeCallCount);
        Assert.Equal(0, runner.ScanCallCount);
        Assert.Equal(0, runner.HealCallCount);
    }

    [Fact]
    public async Task ScanModeDispatchesToRunnerWithDefaultsAndWritesStatistics()
    {
        var runner = new RecordingRunner
        {
            ScanImplementation = async (options, writer, _) =>
            {
                await writer.WriteLineAsync("Files scanned: 2").ConfigureAwait(false);
                await writer.WriteLineAsync("Trades scanned: 4").ConfigureAwait(false);
                await writer.WriteLineAsync("Gaps found: 1").ConfigureAwait(false);
                return 0;
            },
        };
        var sut = new CliApplication(runner);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        var exitCode = await sut.Run(
            ["scan", "btc-usdt"],
            outputWriter,
            errorWriter,
            CancellationToken.None);

        Assert.Equal(0, exitCode);
        Assert.NotNull(runner.ScanOptions);
        Assert.Equal(CliMode.Scan, runner.ScanOptions!.Mode);
        Assert.Equal(Directory.GetCurrentDirectory(), runner.ScanOptions.WorkDirectory);
        Assert.Equal("Binance", runner.ScanOptions.Exchange);
        Assert.Equal("BTC-USDT", runner.ScanOptions.Instrument);
        Assert.Empty(runner.ScanOptions.Dates);
        Assert.Contains("Files scanned: 2", outputWriter.ToString(), StringComparison.Ordinal);
        Assert.Contains("Trades scanned: 4", outputWriter.ToString(), StringComparison.Ordinal);
        Assert.Contains("Gaps found: 1", outputWriter.ToString(), StringComparison.Ordinal);
        Assert.Equal(string.Empty, errorWriter.ToString());
        Assert.Equal(1, runner.ScanCallCount);
        Assert.Equal(0, runner.CandlizeCallCount);
        Assert.Equal(0, runner.HealCallCount);
    }

    [Fact]
    public async Task HealModeDispatchesToRunnerWithAliasOptions()
    {
        var runner = new RecordingRunner
        {
            HealImplementation = async (options, writer, _) =>
            {
                await writer.WriteLineAsync("Gaps healed full: 1").ConfigureAwait(false);
                return 0;
            },
        };
        var sut = new CliApplication(runner);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        var exitCode = await sut.Run(
            ["heal", "btc-usdt", "-on", "20260328", "-x", "Kraken", "-dir", "W:\\Trades"],
            outputWriter,
            errorWriter,
            CancellationToken.None);

        Assert.Equal(0, exitCode);
        Assert.NotNull(runner.HealOptions);
        Assert.Equal(CliMode.Heal, runner.HealOptions!.Mode);
        Assert.Equal("W:\\Trades", runner.HealOptions.WorkDirectory);
        Assert.Equal("Kraken", runner.HealOptions.Exchange);
        Assert.Equal("BTC-USDT", runner.HealOptions.Instrument);
        Assert.Equal([new DateOnly(2026, 3, 28)], runner.HealOptions.Dates);
        Assert.Contains("Gaps healed full: 1", outputWriter.ToString(), StringComparison.Ordinal);
        Assert.Equal(string.Empty, errorWriter.ToString());
        Assert.Equal(1, runner.HealCallCount);
        Assert.Equal(0, runner.CandlizeCallCount);
        Assert.Equal(0, runner.ScanCallCount);
    }

    [Fact]
    public async Task HelpWritesUsageWithoutCallingRunner()
    {
        var runner = new RecordingRunner();
        var sut = new CliApplication(runner);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        var exitCode = await sut.Run(["--help"], outputWriter, errorWriter, CancellationToken.None);

        Assert.Equal(0, exitCode);
        Assert.Contains("Quanta Candle CLI", outputWriter.ToString(), StringComparison.Ordinal);
        Assert.Contains("qc heal <instrument>", outputWriter.ToString(), StringComparison.Ordinal);
        Assert.Contains("--timeFrame", outputWriter.ToString(), StringComparison.Ordinal);
        Assert.Equal(string.Empty, errorWriter.ToString());
        Assert.Equal(0, runner.CandlizeCallCount);
        Assert.Equal(0, runner.ScanCallCount);
        Assert.Equal(0, runner.HealCallCount);
    }

    [Fact]
    public async Task InvalidCliSyntaxWritesErrorAndSkipsRunner()
    {
        var runner = new RecordingRunner();
        var sut = new CliApplication(runner);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        var exitCode = await sut.Run(["heal"], outputWriter, errorWriter, CancellationToken.None);

        Assert.Equal(2, exitCode);
        Assert.Equal(string.Empty, outputWriter.ToString());
        Assert.Contains("instrument argument is required", errorWriter.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, runner.CandlizeCallCount);
        Assert.Equal(0, runner.ScanCallCount);
        Assert.Equal(0, runner.HealCallCount);
    }

    [Fact]
    public async Task RunnerValidationFailureWritesErrorAndReturnsUsageExitCode()
    {
        var runner = new RecordingRunner
        {
            HealImplementation = (_, _, _) => throw new InvalidOperationException("Healing failed."),
        };
        var sut = new CliApplication(runner);
        using var outputWriter = new StringWriter();
        using var errorWriter = new StringWriter();

        var exitCode = await sut.Run(["heal", "btc-usdt"], outputWriter, errorWriter, CancellationToken.None);

        Assert.Equal(2, exitCode);
        Assert.Equal(string.Empty, outputWriter.ToString());
        Assert.Contains("Healing failed.", errorWriter.ToString(), StringComparison.Ordinal);
        Assert.Equal(1, runner.HealCallCount);
    }

    /// <summary>
    /// Records application dispatch calls while allowing each path to simulate console output and exit codes.
    /// </summary>
    private sealed class RecordingRunner : IQuantaCandleRunner
    {
        public Func<CliOptions, TextWriter, CancellationToken, Task<int>> CandlizeImplementation { get; init; }
            = static (_, _, _) => Task.FromResult(0);

        public Func<CliOptions, TextWriter, CancellationToken, Task<int>> HealImplementation { get; init; }
            = static (_, _, _) => Task.FromResult(0);

        public Func<CliOptions, TextWriter, CancellationToken, Task<int>> ScanImplementation { get; init; }
            = static (_, _, _) => Task.FromResult(0);

        public int CandlizeCallCount { get; private set; }

        public int HealCallCount { get; private set; }

        public int ScanCallCount { get; private set; }

        public CliOptions? CandlizeOptions { get; private set; }

        public CliOptions? HealOptions { get; private set; }

        public CliOptions? ScanOptions { get; private set; }

        public Task<int> Candlize(CliOptions runOptions, TextWriter outputWriter, CancellationToken terminator)
        {
            CandlizeCallCount++;
            CandlizeOptions = runOptions;

            var result = CandlizeImplementation(runOptions, outputWriter, terminator);
            return result;
        }

        public Task<int> Heal(CliOptions runOptions, TextWriter outputWriter, CancellationToken terminator)
        {
            HealCallCount++;
            HealOptions = runOptions;

            var result = HealImplementation(runOptions, outputWriter, terminator);
            return result;
        }

        public Task<int> Scan(CliOptions runOptions, TextWriter outputWriter, CancellationToken terminator)
        {
            ScanCallCount++;
            ScanOptions = runOptions;

            var result = ScanImplementation(runOptions, outputWriter, terminator);
            return result;
        }
    }
}
