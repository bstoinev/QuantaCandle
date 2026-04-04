namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Identifies the executable mode selected for the candle generator command-line entrypoint.
/// </summary>
public enum CandleGeneratorMode
{
    /// <summary>
    /// Reads trade files and writes candle files.
    /// </summary>
    GenerateCandles,

    /// <summary>
    /// Scans local trade files and reports numeric trade identifier gaps.
    /// </summary>
    ScanGaps,
}
