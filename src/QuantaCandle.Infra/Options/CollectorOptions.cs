using System;
using System.Collections.Generic;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Infra.Options;

public sealed record CollectorOptions(
    IReadOnlyList<Instrument> Instruments,
    int ChannelCapacity,
    int BatchSize,
    TimeSpan FlushInterval,
    TimeSpan CheckpointInterval,
    int? MaxTradesPerSecond = null,
    int DeduplicationCapacity = 100_000);
