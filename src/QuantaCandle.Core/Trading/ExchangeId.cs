using System;

namespace QuantaCandle.Core.Trading;

public readonly record struct ExchangeId
{
    public ExchangeId(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("ExchangeId cannot be null or whitespace.", nameof(value));
        }

        Value = value.Trim();
    }

    public string Value { get; }

    public override string ToString()
    {
        return Value ?? string.Empty;
    }
}
