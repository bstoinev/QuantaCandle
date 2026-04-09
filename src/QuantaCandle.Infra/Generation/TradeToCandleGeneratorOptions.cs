namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Describes one in-place candle generation request scoped by work directory, exchange, instrument, and optional trading dates.
/// </summary>
public sealed record TradeToCandleGeneratorOptions(
    string WorkDirectory,
    string Exchange,
    string Instrument,
    string Timeframe,
    IReadOnlyList<DateOnly> Dates,
    string Format = "csv");
