namespace QuantaCandle.CLI;

internal interface IQuantaCandleRunner
{
    /// <summary>
    /// Runs candle generation and writes the current summary output.
    /// </summary>
    public Task<int> Candlize(CliOptions runOptions, TextWriter outputWriter, CancellationToken terminator);

    /// <summary>
    /// Runs local gap healing by scanning first and then healing each bounded gap in the requested scope.
    /// </summary>
    public Task<int> Heal(CliOptions runOptions, TextWriter outputWriter, CancellationToken terminator);

    /// <summary>
    /// Runs local gap scanning and writes a per-gap summary without treating gaps as failures.
    /// </summary>
    public Task<int> Scan(CliOptions runOptions, TextWriter outputWriter, CancellationToken terminator);
}
