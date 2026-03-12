using QuantaCandle.Core;

namespace QuantaCandle.Service.Time;

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
