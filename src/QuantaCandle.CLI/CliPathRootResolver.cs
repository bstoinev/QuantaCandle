namespace QuantaCandle.CLI;

/// <summary>
/// Resolves the shared CLI data root directories beneath one working directory.
/// </summary>
internal static class CliPathRootResolver
{
    /// <summary>
    /// Gets the trade-data root beneath the supplied working directory.
    /// </summary>
    public static string GetTradeDataRoot(string workDirectory) => Path.GetFullPath(Path.Combine(workDirectory, "trade-data"));

    /// <summary>
    /// Gets the candle-data root beneath the supplied working directory.
    /// </summary>
    public static string GetCandleDataRoot(string workDirectory) => Path.GetFullPath(Path.Combine(workDirectory, "candle-data"));
}
