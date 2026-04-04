namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Describes a manual candle generator executable run configured from command-line arguments.
/// </summary>
public sealed record CandleGeneratorRunOptions(
    CandleGeneratorMode Mode,
    string Source,
    string Timeframe,
    string InputDirectory,
    string OutputDirectory,
    string Format,
    IReadOnlyList<DateOnly> ScanDates);
