namespace QuantaCandle.Core.Trading;

/// <summary>
/// Attempts to heal one previously detected bounded trade gap for one exchange instrument scope.
/// </summary>
public interface ITradeGapHealer
{
    /// <summary>
    /// Executes local healing for the requested bounded gap and reports whether the fetch achieved full or partial coverage.
    /// </summary>
    ValueTask<TradeGapHealResult> Heal(TradeGapHealRequest request, CancellationToken cancellationToken);
}
