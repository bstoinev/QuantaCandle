namespace QuantaCandle.Core.Trading;

/// <summary>
/// Thrown when an expected last raw trade identifier cannot be verified exactly for a UTC trading day.
/// </summary>
public sealed class TradeDayBoundaryVerificationException : Exception
{
    /// <summary>
    /// Creates a strict-mode trade day boundary verification failure.
    /// </summary>
    public TradeDayBoundaryVerificationException(
        ExchangeId exchange,
        Instrument symbol,
        DateOnly utcDate,
        long candidateExpectedLastTradeId)
        : base(CreateMessage(exchange, symbol, utcDate, candidateExpectedLastTradeId))
    {
        Exchange = exchange;
        Symbol = symbol;
        UtcDate = utcDate;
        CandidateExpectedLastTradeId = candidateExpectedLastTradeId;
    }

    /// <summary>
    /// Gets the exchange whose raw trade identifier could not be verified.
    /// </summary>
    public ExchangeId Exchange { get; }

    /// <summary>
    /// Gets the instrument whose raw trade identifier could not be verified.
    /// </summary>
    public Instrument Symbol { get; }

    /// <summary>
    /// Gets the UTC day whose expected last raw trade identifier could not be verified.
    /// </summary>
    public DateOnly UtcDate { get; }

    /// <summary>
    /// Gets the candidate expected last raw trade identifier that failed verification.
    /// </summary>
    public long CandidateExpectedLastTradeId { get; }

    private static string CreateMessage(ExchangeId exchange, Instrument symbol, DateOnly utcDate, long candidateExpectedLastTradeId)
    {
        var result = $"Unable to verify raw trade id '{candidateExpectedLastTradeId}' as the expected last trade id for {exchange}:{symbol} on UTC day {utcDate:yyyy-MM-dd}.";
        return result;
    }
}
