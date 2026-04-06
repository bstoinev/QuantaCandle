using System.Text;

namespace QuantaCandle.Infra.Pipeline;

/// <summary>
/// Builds the standardized multi-line trade pipeline statistics message.
/// </summary>
public static class TradePipelineStatsLogFormatter
{
    /// <summary>
    /// Formats the supplied statistics snapshot using the recorder's standard output layout.
    /// </summary>
    public static string Format(TradePipelineStatsSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        var builder = new StringBuilder();
        builder.AppendLine($"Trade pipeline statistics:");
        builder.AppendLine($"Trades received: ".PadLeft(22) + snapshot.TradesReceived);
        builder.AppendLine($"Trades written:".PadLeft(22) + snapshot.TradesWritten);
        builder.AppendLine($"Duplicates dropped:".PadLeft(22) + snapshot.DuplicatesDropped);
        builder.AppendLine($"Batches flushed:".PadLeft(22) + snapshot.BatchesFlushed);
        builder.AppendLine($"Min timestamp:".PadLeft(22) + $"{snapshot.MinTimestamp:O}");
        builder.Append($"Max timestamp:".PadLeft(22) + $"{snapshot.MaxTimestamp:O}");

        var result = builder.ToString();
        return result;
    }
}
