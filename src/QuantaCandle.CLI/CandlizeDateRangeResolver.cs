using QuantaCandle.Core.Trading;
using QuantaCandle.Infra;

namespace QuantaCandle.CLI;

/// <summary>
/// Resolves an inclusive candlize UTC date range from explicit arguments and available local daily trade files.
/// </summary>
internal static class CandlizeDateRangeResolver
{
    /// <summary>
    /// Describes the resolved inclusive UTC date range and the discovered local daily files within that range.
    /// </summary>
    internal sealed record Resolution(
        DateOnly BeginDateUtc,
        DateOnly EndDateUtc,
        IReadOnlyList<TradeLocalDailyFilePath.CompletedDayLocalFile> FilesInRange);

    /// <summary>
    /// Resolves the inclusive UTC date range for candlize using the available finalized local daily trade files when needed.
    /// </summary>
    public static Resolution Resolve(
        string tradeRootDirectory,
        string exchange,
        string instrument,
        DateOnly? beginDateUtc,
        DateOnly? endDateUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tradeRootDirectory);
        ArgumentException.ThrowIfNullOrWhiteSpace(exchange);
        ArgumentException.ThrowIfNullOrWhiteSpace(instrument);

        var exchangeId = new ExchangeId(exchange);
        var parsedInstrument = Instrument.Parse(instrument);
        var availableFiles = TradeLocalDailyFilePath
            .DiscoverCompleted(tradeRootDirectory, exchangeId, parsedInstrument)
            .Where(static file => !file.Path.EndsWith(".partial.jsonl", StringComparison.OrdinalIgnoreCase))
            .ToList();
        var requiresDiscovery = beginDateUtc is null || endDateUtc is null;

        if (requiresDiscovery && availableFiles.Count == 0)
        {
            var instrumentDirectory = Path.Combine(tradeRootDirectory, exchangeId.ToString(), parsedInstrument.ToString());
            throw new InvalidOperationException(
                $"Unable to resolve candlize date range for exchange '{exchange}', instrument '{instrument}' because directory '{instrumentDirectory}' has no parseable daily trade files named 'yyyy-MM-dd.jsonl'.");
        }

        var resolvedBeginDateUtc = beginDateUtc ?? availableFiles[0].UtcDate;
        var resolvedEndDateUtc = endDateUtc ?? availableFiles[^1].UtcDate;

        if (resolvedBeginDateUtc > resolvedEndDateUtc)
        {
            throw new ArgumentException(
                $"Invalid candlize date range: begin UTC date '{resolvedBeginDateUtc:yyyy-MM-dd}' cannot be later than end UTC date '{resolvedEndDateUtc:yyyy-MM-dd}'.");
        }

        var filesInRange = availableFiles
            .Where(file => file.UtcDate >= resolvedBeginDateUtc && file.UtcDate <= resolvedEndDateUtc)
            .ToList();
        var result = new Resolution(resolvedBeginDateUtc, resolvedEndDateUtc, filesInRange);
        return result;
    }
}
