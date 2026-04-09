namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Configures when the recorder should request a normal checkpoint based on the in-memory cached trade count.
/// </summary>
public sealed record TradeCheckpointTriggerOptions(int CacheSize);
