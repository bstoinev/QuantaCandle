namespace QuantaCandle.Core.Trading;

/// <summary>
/// Defines the scope for a gap scan without implying that healing will happen.
/// </summary>
public sealed record TradeGapScanRequest(string RootDirectory, IReadOnlyList<TradeGapAffectedFile> CandidateFiles, IReadOnlyList<TradeGapAffectedRange> CandidateRanges)
{
    /// <summary>
    /// Gets the local root directory that contains recorder-style trade JSONL files.
    /// </summary>
    public string RootDirectory { get; } = string.IsNullOrWhiteSpace(RootDirectory)
        ? throw new ArgumentException("RootDirectory cannot be null or whitespace.", nameof(RootDirectory))
        : Path.GetFullPath(RootDirectory.Trim());

    /// <summary>
    /// Gets the candidate files to inspect when the scan is file-backed.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedFile> CandidateFiles { get; } = CandidateFiles ?? throw new ArgumentNullException(nameof(CandidateFiles));

    /// <summary>
    /// Gets the candidate trade ranges to inspect when the scan is range-backed.
    /// </summary>
    public IReadOnlyList<TradeGapAffectedRange> CandidateRanges { get; } = CandidateRanges ?? throw new ArgumentNullException(nameof(CandidateRanges));
}
