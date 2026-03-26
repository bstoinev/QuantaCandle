using System.Text.Json;
using QuantaCandle.Exchange.Binance.Dtos;

namespace QuantaCandle.Infra.Tests.Exchange;

public sealed class BinanceTradeMessageDtoTests
{
    [Fact]
    public void Can_deserialize_trade_message()
    {
        const string json = "{\"e\":\"trade\",\"E\":1773352785119,\"s\":\"BTCUSDT\",\"t\":31156021,\"p\":\"70301.03000000\",\"q\":\"0.00692000\",\"b\":1699822057,\"a\":1699822067,\"T\":1773352785118,\"m\":true,\"M\":true}";

        BinanceTradeMessageDto? dto = JsonSerializer.Deserialize<BinanceTradeMessageDto>(json);

        Assert.NotNull(dto);
        Assert.Equal("trade", dto.EventType);
        Assert.Equal("BTCUSDT", dto.Symbol);
        Assert.Equal(31156021, dto.TradeId);
        Assert.Equal("70301.03000000", dto.Price);
        Assert.Equal("0.00692000", dto.Quantity);
        Assert.Equal(1773352785118, dto.TradeTime);
        Assert.True(dto.BuyerIsMaker);
    }
}
