namespace QuantaCandle.Service.Stubs;

public sealed record CandleGenerationResult(
    int InputTradeCount,
    int UniqueTradeCount,
    int DuplicatesDropped,
    int CandleCount,
    int OutputFileCount);
