namespace QuantaCandle.Infra.Generation;

public sealed record CandleGenerationResult(
    int InputTradeCount,
    int UniqueTradeCount,
    int DuplicatesDropped,
    int CandleCount,
    int OutputFileCount);
