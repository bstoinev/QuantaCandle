/// <summary>
/// Describes one staged file checkpoint emitted during healing.
/// </summary>
internal enum StagingCheckpointKind
{
    Commit,
    MissingRange,
    Page,
}
