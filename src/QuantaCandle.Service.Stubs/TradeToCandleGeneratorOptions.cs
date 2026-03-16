namespace QuantaCandle.Service.Stubs;

public sealed record TradeToCandleGeneratorOptions(
    string InputDirectory,
    string OutputDirectory,
    string Source,
    string Timeframe);
