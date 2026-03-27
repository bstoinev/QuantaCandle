namespace QuantaCandle.Exchange.Binance;

internal static class ExponentialBackoff
{
    public static TimeSpan NextDelay(TimeSpan current, TimeSpan max)
    {
        double nextMs = current.TotalMilliseconds * 2;
        if (nextMs > max.TotalMilliseconds)
        {
            nextMs = max.TotalMilliseconds;
        }

        return TimeSpan.FromMilliseconds(nextMs);
    }
}

