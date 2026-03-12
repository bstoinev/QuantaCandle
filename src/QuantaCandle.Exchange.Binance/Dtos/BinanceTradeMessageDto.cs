using System.Text.Json.Serialization;

namespace QuantaCandle.Exchange.Binance.Dtos;

public sealed record BinanceTradeMessageDto
{
    [JsonPropertyName("e")]
    public string? EventType { get; init; }

    [JsonPropertyName("E")]
    public long EventTime { get; init; }

    [JsonPropertyName("s")]
    public string? Symbol { get; init; }

    [JsonPropertyName("t")]
    public long TradeId { get; init; }

    [JsonPropertyName("p")]
    public string? Price { get; init; }

    [JsonPropertyName("q")]
    public string? Quantity { get; init; }

    [JsonPropertyName("T")]
    public long TradeTime { get; init; }

    [JsonPropertyName("m")]
    public bool BuyerIsMaker { get; init; }
}

