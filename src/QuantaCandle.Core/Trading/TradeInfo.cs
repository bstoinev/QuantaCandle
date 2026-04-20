namespace QuantaCandle.Core.Trading;

/// <summary>
/// Describes one normalized trade record used throughout ingestion and storage flows.
/// </summary>
public readonly record struct TradeInfo
{
    /// <summary>
    /// Initializes a normalized trade record.
    /// </summary>
    public TradeInfo(TradeKey key, DateTimeOffset timestamp, decimal price, decimal quantity, bool buyerIsMaker)
    {
        if (price <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(price), price, "Price must be positive.");
        }

        if (quantity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(quantity), quantity, "Quantity must be positive.");
        }

        Key = key;
        Timestamp = timestamp;
        Price = price;
        Quantity = quantity;
        BuyerIsMaker = buyerIsMaker;
    }

    /// <summary>
    /// Gets the normalized trade key.
    /// </summary>
    public TradeKey Key { get; }

    /// <summary>
    /// Gets the trade timestamp.
    /// </summary>
    public DateTimeOffset Timestamp { get; }

    /// <summary>
    /// Gets the executed trade price.
    /// </summary>
    public decimal Price { get; }

    /// <summary>
    /// Gets the executed base-asset quantity.
    /// </summary>
    public decimal Quantity { get; }

    /// <summary>
    /// Gets a value indicating whether the buyer side was the maker side.
    /// </summary>
    public bool BuyerIsMaker { get; }
}
