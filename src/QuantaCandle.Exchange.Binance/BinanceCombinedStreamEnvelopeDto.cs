using System.Text.Json.Serialization;

namespace QuantaCandle.Exchange.Binance;

public sealed record BinanceCombinedStreamEnvelopeDto<TData>
{
    [JsonPropertyName("stream")]
    public string? Stream { get; init; }

    [JsonPropertyName("data")]
    public TData? Data { get; init; }
}

