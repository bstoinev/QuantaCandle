namespace QuantaCandle.Core.Trading;

/// <summary>
/// Fetches exchange trades for a known bounded missing trade identifier range.
/// </summary>
public interface ITradeGapFetchClient
{
    /// <summary>
    /// Fetches trades for the requested instrument and forwards each downloaded page immediately to the supplied sink.
    /// </summary>
    ValueTask Fetch(
        Instrument instrument,
        long missingTradeIdStart,
        long missingTradeIdEnd,
        ITradeGapFetchedPageSink pageSink,
        ITradeGapProgressReporter? progressReporter,
        CancellationToken cancellationToken);
}
