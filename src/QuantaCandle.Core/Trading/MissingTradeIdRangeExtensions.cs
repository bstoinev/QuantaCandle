namespace QuantaCandle.Core.Trading;

/// <summary>
/// Provides helpers for normalizing missing trade identifier ranges.
/// </summary>
public static class MissingTradeIdRangeExtensions
{
    /// <summary>
    /// Merges ordered or unordered overlapping and adjacent missing trade identifier ranges.
    /// </summary>
    public static IReadOnlyList<MissingTradeIdRange> MergeContiguous(this IReadOnlyList<MissingTradeIdRange> ranges)
    {
        ArgumentNullException.ThrowIfNull(ranges);

        var orderedRanges = ranges
            .OrderBy(static range => range.FirstTradeId)
            .ThenBy(static range => range.LastTradeId)
            .ToArray();
        var result = new List<MissingTradeIdRange>(orderedRanges.Length);

        foreach (var range in orderedRanges)
        {
            if (result.Count == 0)
            {
                result.Add(range);
                continue;
            }

            var previousRange = result[^1];
            if (range.FirstTradeId <= previousRange.LastTradeId + 1)
            {
                result[^1] = new MissingTradeIdRange(
                    previousRange.FirstTradeId,
                    Math.Max(previousRange.LastTradeId, range.LastTradeId));
            }
            else
            {
                result.Add(range);
            }
        }

        return result;
    }
}
