namespace QuantaCandle.Core.Trading;

/// <summary>
/// Represents an inclusive range of missing exchange trade identifiers.
/// </summary>
public readonly record struct MissingTradeIdRange
{
    public MissingTradeIdRange(long firstTradeId, long lastTradeId)
    {
        if (firstTradeId <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(firstTradeId), firstTradeId, "FirstTradeId must be positive.");
        }

        if (lastTradeId < firstTradeId)
        {
            throw new ArgumentOutOfRangeException(nameof(lastTradeId), lastTradeId, "LastTradeId must be greater than or equal to FirstTradeId.");
        }

        FirstTradeId = firstTradeId;
        LastTradeId = lastTradeId;
    }

    public long FirstTradeId { get; }

    public long LastTradeId { get; }
}
