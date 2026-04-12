namespace QuantaCandle.Core.Trading;

/// <summary>
/// Reports structured trade gap healing progress updates.
/// </summary>
public interface ITradeGapProgressReporter
{
    /// <summary>
    /// Reports one progress update for the current healing flow.
    /// </summary>
    ValueTask Report(TradeGapProgressUpdate update, CancellationToken cancellationToken);
}
