namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Identifies the CLI command selected for the candle generator entrypoint.
/// </summary>
public enum CandleGeneratorMode
{
    /// <summary>
    /// Reads trade files and writes candle files.
    /// </summary>
    Candlize,

    /// <summary>
    /// Scans local trade files and reports numeric trade identifier gaps.
    /// </summary>
    Scan,

    /// <summary>
    /// Scans local trade files and heals bounded numeric trade identifier gaps.
    /// </summary>
    Heal,
}
