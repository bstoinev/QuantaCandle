namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Describes the type of recorder checkpoint request that was published.
/// </summary>
public enum CheckpointRequestKind
{
    /// <summary>
    /// Runs the normal checkpoint flow only.
    /// </summary>
    Checkpoint,

    /// <summary>
    /// Runs the normal checkpoint flow and then exports an ad-hoc scratch snapshot.
    /// </summary>
    Snapshot,
}
