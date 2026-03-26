using QuantaCandle.Service.Stubs;

namespace QuantaCandle.CLI;

/// <summary>
/// Holds the sink-specific registrations for the collector composition root.
/// </summary>
public sealed record TradeSinkRegistration(TradeSinkFileSimpleOptions? FileOptions, TradeSinkS3SimpleOptions? S3Options);
