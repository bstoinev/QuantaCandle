namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Executes the candle generation workflow for a parsed command-line request.
/// </summary>
public interface ICandleGenerationRunner
{
    /// <summary>
    /// Generates candles from local trade files according to the supplied options.
    /// </summary>
    Task<CandleGenerationResult> GenerateAsync(TradeToCandleGeneratorOptions options, CancellationToken cancellationToken);
}
