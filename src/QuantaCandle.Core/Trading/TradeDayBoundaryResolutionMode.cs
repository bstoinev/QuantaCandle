namespace QuantaCandle.Core.Trading;

/// <summary>
/// Controls how expected last raw trade identifier verification failures are handled.
/// </summary>
public enum TradeDayBoundaryResolutionMode
{
    /// <summary>
    /// Throws when the expected last raw trade identifier cannot be verified exactly.
    /// </summary>
    Strict = 1,

    /// <summary>
    /// Returns an unresolved boundary result instead of throwing when the expected last raw trade identifier cannot be verified exactly.
    /// </summary>
    BestEffort = 2,
}
