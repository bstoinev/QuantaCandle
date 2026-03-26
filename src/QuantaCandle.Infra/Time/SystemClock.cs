using QuantaCandle.Core;

namespace QuantaCandle.Infra.Time;

public sealed class SystemClock : IClock
{
    public DateTimeOffset UtcNow
    {
        get
        {
            return DateTimeOffset.UtcNow;
        }
    }
}
