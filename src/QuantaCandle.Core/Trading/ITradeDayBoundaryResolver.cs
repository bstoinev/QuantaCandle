namespace QuantaCandle.Core.Trading;

/// <summary>
/// Resolves expected raw trade identifier boundaries for one UTC trading day.
/// </summary>
public interface ITradeDayBoundaryResolver
{
    /// <summary>
    /// Resolves the expected first raw trade identifier for the requested day and attempts to resolve the expected last raw trade identifier.
    /// </summary>
    ValueTask<TradeDayBoundary> Resolve(
        ExchangeId exchange,
        Instrument symbol,
        DateOnly utcDate,
        TradeDayBoundaryResolutionMode resolutionMode,
        CancellationToken cancellationToken);
}
