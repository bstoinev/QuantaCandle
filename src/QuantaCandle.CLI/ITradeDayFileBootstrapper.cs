using QuantaCandle.Core.Trading;

namespace QuantaCandle.CLI;

/// <summary>
/// Bootstraps one missing explicit local UTC day file so it can flow through the normal per-file healing pipeline.
/// </summary>
internal interface ITradeDayFileBootstrapper
{
    /// <summary>
    /// Resolves a strict Binance anchor trade for the requested UTC day and creates the standard local JSONL day file when needed.
    /// </summary>
    ValueTask<TradeGapAffectedFile> Bootstrap(
        string tradeRootDirectory,
        ExchangeId exchange,
        Instrument symbol,
        DateOnly utcDate,
        CancellationToken cancellationToken);
}
