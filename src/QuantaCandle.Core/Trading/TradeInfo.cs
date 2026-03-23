namespace QuantaCandle.Core.Trading;

public readonly record struct TradeInfo
{
    public TradeInfo(TradeKey key, DateTimeOffset timestamp, decimal price, decimal quantity)
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
    }

    public TradeKey Key { get; }

    public DateTimeOffset Timestamp { get; }

    public decimal Price { get; }

    public decimal Quantity { get; }
}
