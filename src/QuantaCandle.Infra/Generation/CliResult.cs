namespace QuantaCandle.Infra.Generation;

public sealed record CliResult(
    int InputTradeCount,
    int UniqueTradeCount,
    int DuplicatesDropped,
    int CandleCount,
    int OutputFileCount);
