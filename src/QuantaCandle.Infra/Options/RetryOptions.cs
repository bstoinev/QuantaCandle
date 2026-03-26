using System;

namespace QuantaCandle.Infra.Options;

public sealed record RetryOptions(TimeSpan InitialDelay, TimeSpan MaxDelay);

