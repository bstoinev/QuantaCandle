using System;

namespace QuantaCandle.Core;

/// <summary>
/// Provides time in a deterministic way.
/// </summary>
public interface IClock
{
    DateTimeOffset UtcNow { get; }
}
