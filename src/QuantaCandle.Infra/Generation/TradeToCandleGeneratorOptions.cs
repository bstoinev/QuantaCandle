namespace QuantaCandle.Infra.Generation;

public sealed record TradeToCandleGeneratorOptions(
    string InputDirectory,
    string OutputDirectory,
    string Source,
    string Timeframe,
    string Format = "csv");
