using System;

namespace QuantaCandle.Infra.Pipeline;

public sealed record TradePipelineStatsSnapshot(
    long TradesReceived,
    long TradesWritten,
    long DuplicatesDropped,
    long BatchesFlushed,
    DateTimeOffset? MinTimestamp,
    DateTimeOffset? MaxTimestamp);
