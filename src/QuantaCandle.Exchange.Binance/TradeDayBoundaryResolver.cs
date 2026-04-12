using System.Globalization;

using LogMachina;

using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance.Internal;

namespace QuantaCandle.Exchange.Binance;

/// <summary>
/// Resolves expected Binance raw trade identifier boundaries for one UTC day.
/// </summary>
public sealed class TradeDayBoundaryResolver(
    IBinanceRawTradeLookupClient rawTradeLookupClient,
    ILogMachina<TradeDayBoundaryResolver> log) : ITradeDayBoundaryResolver
{
    private readonly ILogMachina<TradeDayBoundaryResolver> _log = log ?? throw new ArgumentNullException(nameof(log));
    private readonly IBinanceRawTradeLookupClient _rawTradeLookupClient = rawTradeLookupClient ?? throw new ArgumentNullException(nameof(rawTradeLookupClient));

    /// <summary>
    /// Resolves the expected first raw trade identifier and attempts to verify the expected last raw trade identifier for a UTC day.
    /// </summary>
    public async ValueTask<TradeDayBoundary> Resolve(
        ExchangeId exchange,
        Instrument symbol,
        DateOnly utcDate,
        TradeDayBoundaryResolutionMode resolutionMode,
        CancellationToken cancellationToken)
    {
        ValidateExchange(exchange);

        var utcDayStart = CreateUtcDayStart(utcDate);
        var nextUtcDayStart = utcDayStart.AddDays(1);
        _log.Trace($"Resolving Binance raw trade day boundary for {symbol} on {utcDate:yyyy-MM-dd}.");

        var firstTrade = await _rawTradeLookupClient.FindFirstTradeAt(utcDayStart, symbol, cancellationToken).ConfigureAwait(false);
        ValidateTradeWithinDay(firstTrade, utcDate, "first");

        var nextDayFirstTrade = await _rawTradeLookupClient.FindFirstTradeAt(nextUtcDayStart, symbol, cancellationToken).ConfigureAwait(false);
        var expectedFirstTradeId = ParseTradeId(firstTrade);
        var nextDayFirstTradeId = ParseTradeId(nextDayFirstTrade);
        var candidateExpectedLastTradeId = nextDayFirstTradeId - 1;
        _log.Debug($"Resolved Binance raw first trade id {expectedFirstTradeId} for {symbol} on {utcDate:yyyy-MM-dd}; candidate last trade id is {candidateExpectedLastTradeId}.");

        var verified = await _rawTradeLookupClient.TryVerifyRawTradeId(symbol, candidateExpectedLastTradeId, cancellationToken).ConfigureAwait(false);

        TradeDayBoundary result;
        if (verified)
        {
            _log.Info($"Resolved Binance raw trade boundaries for {symbol} on {utcDate:yyyy-MM-dd}: {expectedFirstTradeId}-{candidateExpectedLastTradeId}.");
            result = new TradeDayBoundary(exchange, symbol, utcDate, expectedFirstTradeId, candidateExpectedLastTradeId, null);
        }
        else if (resolutionMode == TradeDayBoundaryResolutionMode.Strict)
        {
            _log.Warn($"Unable to verify Binance raw candidate last trade id {candidateExpectedLastTradeId} for {symbol} on {utcDate:yyyy-MM-dd} in strict mode.");
            throw new TradeDayBoundaryVerificationException(exchange, symbol, utcDate, candidateExpectedLastTradeId);
        }
        else
        {
            var warning = CreateVerificationWarning(symbol, utcDate, candidateExpectedLastTradeId);
            _log.Warn(warning);
            result = new TradeDayBoundary(exchange, symbol, utcDate, expectedFirstTradeId, null, warning);
        }

        return result;
    }

    private static DateTimeOffset CreateUtcDayStart(DateOnly utcDate)
    {
        var result = new DateTimeOffset(utcDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc));
        return result;
    }

    private static string CreateVerificationWarning(Instrument symbol, DateOnly utcDate, long candidateExpectedLastTradeId)
    {
        var result = $"Unable to verify Binance raw candidate last trade id {candidateExpectedLastTradeId} for {symbol} on {utcDate:yyyy-MM-dd}.";
        return result;
    }

    private static long ParseTradeId(TradeInfo trade)
    {
        if (!long.TryParse(trade.Key.TradeId, NumberStyles.None, CultureInfo.InvariantCulture, out var result))
        {
            throw new InvalidOperationException($"Trade '{trade.Key.TradeId}' is not numeric.");
        }

        return result;
    }

    private static void ValidateExchange(ExchangeId exchange)
    {
        if (!exchange.Value.Equals(BinanceHelper.Signature.Value, StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException($"Trade day boundary resolution supports exchange '{BinanceHelper.Signature}' only. Actual '{exchange}'.");
        }
    }

    private static void ValidateTradeWithinDay(TradeInfo trade, DateOnly utcDate, string boundaryName)
    {
        var tradeUtcDate = DateOnly.FromDateTime(trade.Timestamp.UtcDateTime);
        if (tradeUtcDate != utcDate)
        {
            throw new InvalidOperationException($"The {boundaryName} Binance raw trade at or after UTC day start resolved to {trade.Timestamp:O}, which is outside UTC day {utcDate:yyyy-MM-dd}.");
        }
    }
}
