namespace QuantaCandle.Infra.Storage;

/// <summary>
/// Observes staged file checkpoints without altering production write semantics.
/// </summary>
internal interface IStagingObserver
{
    /// <summary>
    /// Observes a staged file checkpoint after its contents were flushed to disk.
    /// </summary>
    ValueTask OnCheckpoint(string tempPath, StagingCheckpointKind checkpointKind, CancellationToken cancellationToken);
}

