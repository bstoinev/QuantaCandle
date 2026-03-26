namespace QuantaCandle.Exchange.Binance;

public sealed record BinanceTradeSourceOptions(
    string BaseWebSocketUrl,
    TimeSpan InitialReconnectDelay,
    TimeSpan MaxReconnectDelay,
    int ReceiveBufferSize)
{
    public static BinanceTradeSourceOptions Default
    {
        get => new BinanceTradeSourceOptions(
              BaseWebSocketUrl: "wss://stream.binance.com:9443",
              InitialReconnectDelay: TimeSpan.FromSeconds(1),
              MaxReconnectDelay: TimeSpan.FromSeconds(30),
              ReceiveBufferSize: 16 * 1024);
    }
}

