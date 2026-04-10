namespace QuantaCandle.CLI;

/// <summary>
/// Adapts <see cref="TradeToCandleGenerator"/> to the executable dispatch surface.
/// </summary>
public sealed class TradeToCandleGenerationRunner : ICandleGenerationRunner
{
    /// <summary>
    /// Runs the candle generation pipeline.
    /// </summary>
    public Task<CliResult> GenerateAsync(CliOptions options, CancellationToken cancellationToken) => TradeToCandleGenerator.Run(options, cancellationToken);
}
