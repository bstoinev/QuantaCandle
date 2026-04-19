using QuantaCandle.Core.Trading;

/// <summary>
/// Represents candidate file resolution for an optionally date-scoped scan or heal request.
/// </summary>
internal sealed record CandidateFileResolution(
    IReadOnlyList<TradeGapAffectedFile> ResolvedFiles,
    IReadOnlyList<TradeGapAffectedFile> MissingFiles,
    IReadOnlyList<DateOnly> MissingDates,
    string ExpectedPathExample);
