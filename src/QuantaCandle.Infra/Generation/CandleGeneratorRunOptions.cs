namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Describes a manual candle generation execution configured from command-line arguments.
/// </summary>
public sealed record CandleGeneratorRunOptions(string Source, string Timeframe, string InputDirectory, string OutputDirectory, string Format);
