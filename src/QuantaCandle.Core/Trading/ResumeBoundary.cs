namespace QuantaCandle.Core.Trading;

/// <summary>
/// Represents the startup recovery lower bound used to resume ingestion for one instrument stream.
/// </summary>
public readonly record struct ResumeBoundary
{
    /// <summary>
    /// Creates a startup recovery lower bound.
    /// </summary>
    public ResumeBoundary(DateTimeOffset timestampUtc, DateOnly utcDate, string origin)
    {
        if (timestampUtc == default)
        {
            throw new ArgumentException("TimestampUtc must be non-default.", nameof(timestampUtc));
        }

        if (timestampUtc.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("TimestampUtc must be expressed in UTC.", nameof(timestampUtc));
        }

        if (utcDate != DateOnly.FromDateTime(timestampUtc.UtcDateTime))
        {
            throw new ArgumentException("UtcDate must match TimestampUtc.", nameof(utcDate));
        }

        if (string.IsNullOrWhiteSpace(origin))
        {
            throw new ArgumentException("Origin cannot be null or whitespace.", nameof(origin));
        }

        TimestampUtc = timestampUtc;
        UtcDate = utcDate;
        Origin = origin.Trim();
    }

    /// <summary>
    /// Gets the lower-bound resume timestamp in UTC.
    /// </summary>
    public DateTimeOffset TimestampUtc { get; }

    /// <summary>
    /// Gets the UTC date that owns the boundary timestamp.
    /// </summary>
    public DateOnly UtcDate { get; }

    /// <summary>
    /// Gets the source that produced the boundary.
    /// </summary>
    public string Origin { get; }
}
