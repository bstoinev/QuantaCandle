namespace QuantaCandle.CLI;

/// <summary>
/// Identifies the CLI command selected for the CLI entrypoint.
/// </summary>
public enum CliMode
{
    Unknown,

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
