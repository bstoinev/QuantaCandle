namespace QuantaCandle.Core.Trading;

public readonly record struct TradeWatermark
{
    public TradeWatermark(string tradeId, DateTimeOffset timestamp)
    {
        if (string.IsNullOrWhiteSpace(tradeId))
        {
            throw new ArgumentException("TradeId cannot be null or whitespace.", nameof(tradeId));
        }

        if (timestamp == default)
        {
            throw new ArgumentException("Timestamp must be non-default.", nameof(timestamp));
        }

        TradeId = tradeId.Trim();
        Timestamp = timestamp;
    }

    public string TradeId { get; }

    public DateTimeOffset Timestamp { get; }
}
