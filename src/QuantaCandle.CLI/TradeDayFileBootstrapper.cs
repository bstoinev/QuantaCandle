using LogMachina;

using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance;
using QuantaCandle.Infra;

namespace QuantaCandle.CLI;

/// <summary>
/// Creates a missing explicit UTC day file by seeding it with a strict Binance anchor trade.
/// </summary>
internal sealed class TradeDayFileBootstrapper(
    ITradeDayBoundaryResolver tradeDayBoundaryResolver,
    IBinanceRawTradeLookupClient binanceRawTradeLookupClient,
    ILogMachina<TradeDayFileBootstrapper> log) : ITradeDayFileBootstrapper
{
    private readonly IBinanceRawTradeLookupClient _binanceRawTradeLookupClient = binanceRawTradeLookupClient ?? throw new ArgumentNullException(nameof(binanceRawTradeLookupClient));
    private readonly ILogMachina<TradeDayFileBootstrapper> _log = log ?? throw new ArgumentNullException(nameof(log));
    private readonly ITradeDayBoundaryResolver _tradeDayBoundaryResolver = tradeDayBoundaryResolver ?? throw new ArgumentNullException(nameof(tradeDayBoundaryResolver));

    /// <summary>
    /// Resolves strict UTC day boundaries, fetches the first raw trade of the day, and creates the missing local file if it does not already exist.
    /// </summary>
    public async ValueTask<TradeGapAffectedFile> Bootstrap(
        string tradeRootDirectory,
        ExchangeId exchange,
        Instrument symbol,
        DateOnly utcDate,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tradeRootDirectory);

        var fullPath = TradeLocalDailyFilePath.Build(tradeRootDirectory, exchange, symbol, utcDate);
        var relativePath = Path.GetRelativePath(tradeRootDirectory, fullPath);
        var result = new TradeGapAffectedFile(relativePath, utcDate);

        if (File.Exists(fullPath))
        {
            _log.Debug($"Skipping bootstrap for existing trade day file '{relativePath}'.");
            return result;
        }

        _log.Trace($"Bootstrapping missing trade day file '{relativePath}' for {exchange}:{symbol} on {utcDate:yyyy-MM-dd}.");
        _ = await _tradeDayBoundaryResolver
            .Resolve(exchange, symbol, utcDate, TradeDayBoundaryResolutionMode.Strict, cancellationToken)
            .ConfigureAwait(false);

        TradeInfo anchorTrade;
        try
        {
            var utcDayStart = new DateTimeOffset(utcDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc));
            anchorTrade = await _binanceRawTradeLookupClient
                .FindFirstTradeAt(utcDayStart, symbol, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _log.Warn($"Strict UTC day boundaries resolved for {exchange}:{symbol} on {utcDate:yyyy-MM-dd}, but anchor trade lookup failed for '{relativePath}'.");
            _log.Error(ex);
            _log.Debug($"Bootstrap target file='{relativePath}', exchange='{exchange}', instrument='{symbol}', utcDate='{utcDate:yyyy-MM-dd}'.");
            throw new InvalidOperationException(
                $"Unable to bootstrap missing trade day file '{relativePath}' because no Binance anchor trade could be resolved for UTC day {utcDate:yyyy-MM-dd}.",
                ex);
        }

        var anchorTradeUtcDate = DateOnly.FromDateTime(anchorTrade.Timestamp.UtcDateTime);
        if (anchorTradeUtcDate != utcDate)
        {
            throw new InvalidOperationException(
                $"Unable to bootstrap missing trade day file '{relativePath}' because the resolved Binance anchor trade timestamp {anchorTrade.Timestamp:O} is outside UTC day {utcDate:yyyy-MM-dd}.");
        }

        await TradeJsonlFile
            .WriteFullPayload(fullPath, TradeJsonlFile.BuildPayload([anchorTrade]), cancellationToken)
            .ConfigureAwait(false);

        _log.Info($"Bootstrapped missing trade day file '{relativePath}' with Binance anchor trade {anchorTrade.Key.TradeId} for {exchange}:{symbol} on {utcDate:yyyy-MM-dd}.");
        return result;
    }
}
