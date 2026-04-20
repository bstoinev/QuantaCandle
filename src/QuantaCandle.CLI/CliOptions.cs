namespace QuantaCandle.CLI;

/// <summary>
/// Describes one in-place candle generation request scoped by work directory, exchange, instrument, and optional trading dates.
/// </summary>
public sealed record CliOptions(
    CliMode Mode,
    string WorkDirectory,
    string Exchange,
    string Instrument,
    string Timeframe,
    IReadOnlyList<DateOnly> Dates,
    string Format = "csv",
    DateOnly? BeginDateUtc = null,
    DateOnly? EndDateUtc = null);
