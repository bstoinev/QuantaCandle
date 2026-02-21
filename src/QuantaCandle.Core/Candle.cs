namespace QuantaCandle.Core;

public class Candle
{
    public required DateTime Timestamp { get; set; }
    public required decimal Open { get; set; }
    public required decimal High { get; set; }
    public required decimal Low { get; set; }
    public required decimal Close { get; set; }
    public required decimal Volume { get; set; }
}
