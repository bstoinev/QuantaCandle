namespace QuantaCandle.Infra;

/// <summary>
/// Builds the deterministic S3 object key used for one finalized instrument-day trade file.
/// </summary>
public static class TradeSinkS3DailyObjectKey
{
    /// <summary>
    /// Creates the S3 object key using the configured prefix, instrument, and UTC calendar date.
    /// The key remains suitable for finalized daily uploads even while active-day buffering stays in memory.
    /// </summary>
    public static string Build(string? prefix, string exchange, string instrument, DateOnly utcDate)
    {
        var result = BuildForFileName(prefix, exchange, instrument, $"{utcDate:yyyy-MM-dd}.jsonl");
        return result;
    }

    /// <summary>
    /// Creates the S3 object key using the configured prefix, instrument, and finalized local file name.
    /// </summary>
    public static string BuildForFileName(string? prefix, string exchange, string instrument, string finalizedFileName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(finalizedFileName);

        var normalizedPrefix = NormalizePrefix(prefix);
        var objectPath = $"{exchange}/{instrument}/{finalizedFileName}";
        var result = string.IsNullOrEmpty(normalizedPrefix)
            ? objectPath
            : $"{normalizedPrefix}/{objectPath}";

        return result;
    }

    private static string NormalizePrefix(string? prefix)
    {
        var result = string.IsNullOrWhiteSpace(prefix)
            ? string.Empty
            : prefix.Replace('\\', '/').Trim('/');

        return result;
    }
}
