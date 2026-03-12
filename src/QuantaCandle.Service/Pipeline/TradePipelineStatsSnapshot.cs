using System;

namespace QuantaCandle.Service.Pipeline;

public sealed record TradePipelineStatsSnapshot(
    long TradesReceived,
    long TradesWritten,
    long BatchesFlushed,
    DateTimeOffset? MinTimestamp,
    DateTimeOffset? MaxTimestamp);

