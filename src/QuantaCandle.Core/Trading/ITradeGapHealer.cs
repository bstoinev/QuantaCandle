namespace QuantaCandle.Core.Trading;

/// <summary>
/// Attempts to heal previously detected trade gaps for one exchange instrument scope.
/// </summary>
public interface ITradeGapHealer
{
    /// <summary>
    /// Executes healing for the requested gaps and reports healed and unresolved outcomes separately.
    /// </summary>
    ValueTask<TradeGapHealResult> Heal(TradeGapHealRequest request, CancellationToken cancellationToken);
}
