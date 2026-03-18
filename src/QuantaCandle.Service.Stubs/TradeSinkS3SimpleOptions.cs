namespace QuantaCandle.Service.Stubs;

/// <summary>
/// Configuration for <see cref="TradeSinkS3Simple"/>.
/// </summary>
public sealed record TradeSinkS3SimpleOptions(string BucketName, string? Prefix);
