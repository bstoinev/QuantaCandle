using QuantaCandle.Infra.Options;

namespace QuantaCandle.Infra;

/// <summary>
/// Describes a recorder execution configured from command-line arguments.
/// </summary>
public sealed record TradeRecorderRunOptions(
    TimeSpan Duration,
    CollectorOptions CollectorOptions,
    RetryOptions RetryOptions,
    TradeRecorderSourceRegistration SourceRegistration,
    TradeRecorderSinkRegistration SinkRegistration);
