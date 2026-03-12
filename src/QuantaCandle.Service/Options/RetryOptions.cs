using System;

namespace QuantaCandle.Service.Options;

public sealed record RetryOptions(TimeSpan InitialDelay, TimeSpan MaxDelay);

