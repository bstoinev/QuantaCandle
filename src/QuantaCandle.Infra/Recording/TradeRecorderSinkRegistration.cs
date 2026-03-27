namespace QuantaCandle.Infra;

/// <summary>
/// Holds the sink-specific registrations for the trade recorder container.
/// </summary>
public sealed record TradeRecorderSinkRegistration(TradeSinkFileSimpleOptions? FileOptions, TradeSinkS3SimpleOptions? S3Options);
