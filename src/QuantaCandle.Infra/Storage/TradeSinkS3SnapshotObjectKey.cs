namespace QuantaCandle.Infra;

/// <summary>
/// Builds deterministic S3 object keys for ad-hoc scratch snapshots.
/// </summary>
public static class TradeSinkS3SnapshotObjectKey
{
    /// <summary>
    /// Builds the S3 object key for one instrument scratch snapshot artifact.
    /// </summary>
    public static string Build(string? prefix, string instrument, string snapshotFileName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instrument);
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotFileName);

        var trimmedPrefix = prefix?.Trim().Trim('/');
        var result = string.IsNullOrWhiteSpace(trimmedPrefix)
            ? $"{instrument}/{snapshotFileName}"
            : $"{trimmedPrefix}/{instrument}/{snapshotFileName}";
        return result;
    }
}
