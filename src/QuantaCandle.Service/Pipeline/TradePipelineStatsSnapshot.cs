using System;

namespace QuantaCandle.Service.Pipeline;

public sealed record TradePipelineStatsSnapshot(
    long TradesReceived,
    long TradesWritten,
    long DuplicatesDropped,
    long BatchesFlushed,
    DateTimeOffset? MinTimestamp,
    DateTimeOffset? MaxTimestamp);
