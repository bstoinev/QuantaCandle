namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Describes one CLI run configured from command-line arguments.
/// </summary>
public sealed record CandleGeneratorRunOptions(
    CandleGeneratorMode Mode,
    string Exchange,
    string Instrument,
    string WorkDirectory,
    IReadOnlyList<DateOnly> Dates);
