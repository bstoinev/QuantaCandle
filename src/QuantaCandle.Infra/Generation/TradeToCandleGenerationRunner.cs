namespace QuantaCandle.Infra.Generation;

/// <summary>
/// Adapts <see cref="TradeToCandleGenerator"/> to the executable dispatch surface.
/// </summary>
public sealed class TradeToCandleGenerationRunner : ICandleGenerationRunner
{
    private readonly TradeToCandleGenerator _generator = new();

    /// <summary>
    /// Runs the candle generation pipeline.
    /// </summary>
    public Task<CliResult> GenerateAsync(CliOptions options, CancellationToken cancellationToken)
    {
        var result = _generator.Run(options, cancellationToken);
        return result;
    }
}
