namespace QuantaCandle.Core.Trading;

/// <summary>
/// Represents the expected raw trade identifier boundaries for one UTC trading day.
/// </summary>
public sealed record TradeDayBoundary
{
    /// <summary>
    /// Creates a UTC day boundary resolution result.
    /// </summary>
    public TradeDayBoundary(
        ExchangeId exchange,
        Instrument symbol,
        DateOnly utcDate,
        long expectedFirstTradeId,
        long? expectedLastTradeId,
        string? warning)
    {
        if (expectedFirstTradeId <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(expectedFirstTradeId), expectedFirstTradeId, "Expected first trade id must be positive.");
        }

        if (expectedLastTradeId is not null && expectedLastTradeId.Value < expectedFirstTradeId)
        {
            throw new ArgumentOutOfRangeException(nameof(expectedLastTradeId), expectedLastTradeId, "Expected last trade id must be greater than or equal to expected first trade id.");
        }

        Exchange = exchange;
        Symbol = symbol;
        UtcDate = utcDate;
        ExpectedFirstTradeId = expectedFirstTradeId;
        ExpectedLastTradeId = expectedLastTradeId;
        Warning = string.IsNullOrWhiteSpace(warning) ? null : warning.Trim();
    }

    /// <summary>
    /// Gets the exchange that owns the raw trade identifiers.
    /// </summary>
    public ExchangeId Exchange { get; }

    /// <summary>
    /// Gets the instrument that owns the raw trade identifiers.
    /// </summary>
    public Instrument Symbol { get; }

    /// <summary>
    /// Gets the UTC date whose boundaries were resolved.
    /// </summary>
    public DateOnly UtcDate { get; }

    /// <summary>
    /// Gets the first raw trade identifier at or after the UTC day start.
    /// </summary>
    public long ExpectedFirstTradeId { get; }

    /// <summary>
    /// Gets the verified last raw trade identifier for the UTC day when verification succeeds.
    /// </summary>
    public long? ExpectedLastTradeId { get; }

    /// <summary>
    /// Gets a warning message when the expected last raw trade identifier could not be verified.
    /// </summary>
    public string? Warning { get; }

    /// <summary>
    /// Gets a value indicating whether the expected last raw trade identifier was verified successfully.
    /// </summary>
    public bool HasExpectedLastTradeId => ExpectedLastTradeId.HasValue;
}
