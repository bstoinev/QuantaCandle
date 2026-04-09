namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Carries one published checkpoint request to listeners.
/// </summary>
public sealed record CheckpointSignalNotification(long Version, CheckpointRequestKind RequestKind);
