using System.Globalization;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Storage;

/// <summary>
/// Creates local repair plans for missing UTC day start and end raw-trade boundaries.
/// </summary>
public static class TradeDayBoundaryGapPlanner
{
    /// <summary>
    /// Creates a repair plan for a missing UTC day start boundary when the local file begins after the resolved first raw trade identifier.
    /// </summary>
    public static (TradeGap Gap, TradeGapAffectedRange AffectedRange)? CreateStartBoundaryGap(
        TradeDayBoundary boundary,
        string localFirstTradeId,
        long localFirstNumericTradeId,
        DateTimeOffset localFirstTimestamp,
        string relativeFilePath,
        int lineNumber,
        DateTimeOffset observedAt)
    {
        (TradeGap Gap, TradeGapAffectedRange AffectedRange)? result = null;

        if (localFirstNumericTradeId > boundary.ExpectedFirstTradeId)
        {
            var missingTradeIds = new MissingTradeIdRange(boundary.ExpectedFirstTradeId, localFirstNumericTradeId - 1);
            var syntheticLowerBoundary = new TradeWatermark(
                (boundary.ExpectedFirstTradeId - 1L).ToString(CultureInfo.InvariantCulture),
                CreateUtcDayStart(boundary.UtcDate));
            var localWatermark = new TradeWatermark(localFirstTradeId, localFirstTimestamp);
            var gap = TradeGap
                .CreateOpen(Guid.NewGuid(), boundary.Exchange, boundary.Symbol, syntheticLowerBoundary, observedAt)
                .ToBounded(localWatermark, missingTradeIds);
            var affectedRange = new TradeGapAffectedRange(
                localWatermark,
                localWatermark,
                new TradeGapBoundaryLocation(relativeFilePath, lineNumber),
                new TradeGapBoundaryLocation(relativeFilePath, lineNumber));

            result = (gap, affectedRange);
        }

        return result;
    }

    /// <summary>
    /// Creates a repair plan for a missing UTC day end boundary when the local file ends before the resolved last raw trade identifier.
    /// </summary>
    public static (TradeGap Gap, TradeGapAffectedRange AffectedRange)? CreateEndBoundaryGap(
        TradeDayBoundary boundary,
        string localLastTradeId,
        long localLastNumericTradeId,
        DateTimeOffset localLastTimestamp,
        string relativeFilePath,
        int lineNumber,
        DateTimeOffset observedAt)
    {
        (TradeGap Gap, TradeGapAffectedRange AffectedRange)? result = null;

        if (boundary.HasExpectedLastTradeId
            && localLastNumericTradeId < boundary.ExpectedLastTradeId!.Value)
        {
            var missingTradeIds = new MissingTradeIdRange(localLastNumericTradeId + 1, boundary.ExpectedLastTradeId.Value);
            var localWatermark = new TradeWatermark(localLastTradeId, localLastTimestamp);
            var syntheticUpperBoundary = new TradeWatermark(
                boundary.ExpectedLastTradeId.Value.ToString(CultureInfo.InvariantCulture),
                CreateUtcDayStart(boundary.UtcDate).AddDays(1));
            var gap = TradeGap
                .CreateOpen(Guid.NewGuid(), boundary.Exchange, boundary.Symbol, localWatermark, observedAt)
                .ToBounded(syntheticUpperBoundary, missingTradeIds);
            var affectedRange = new TradeGapAffectedRange(
                localWatermark,
                localWatermark,
                new TradeGapBoundaryLocation(relativeFilePath, lineNumber),
                new TradeGapBoundaryLocation(relativeFilePath, lineNumber));

            result = (gap, affectedRange);
        }

        return result;
    }

    private static DateTimeOffset CreateUtcDayStart(DateOnly tradingDay)
    {
        var result = new DateTimeOffset(tradingDay.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc));
        return result;
    }
}
