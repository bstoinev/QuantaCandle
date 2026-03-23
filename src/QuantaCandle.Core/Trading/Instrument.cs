using System.Diagnostics;

namespace QuantaCandle.Core.Trading;

[DebuggerDisplay($"{{{nameof(GetDebuggerDisplay)}(),nq}}")]
public readonly record struct Instrument : IEquatable<Instrument>
{
    public static implicit operator Instrument(string expression)
    {
        return Parse(expression);
    }

    public static implicit operator string(Instrument pair)
    {
        return pair.ToString();
    }

    /// <summary>
    /// Converts the string representation of a pair to a TradingAsset.
    /// </summary>
    /// <param name="baseDASHquote">The string representation of the pair in format AssetSymbol-CurrencySymbol</param>
    /// <returns></returns>
    public static Instrument Parse(string baseDASHquote)
    {
        ArgumentNullException.ThrowIfNull(baseDASHquote);

        var parts = baseDASHquote.Split('-');

        if (parts.Length != 2)
        {
            throw new ArgumentException("The specified argument contains invalid format. Only strings in format BASE-QUOTE are accepted.");
        }

        return new Instrument
        {
            BaseSymbol = parts[0].ToUpperInvariant(),
            QuoteSymbol = parts[1].ToUpperInvariant(),
        };
    }

    public required string BaseSymbol { get; init; }

    public required string QuoteSymbol { get; init; }

    public override string ToString() => $"{BaseSymbol}-{QuoteSymbol}";

    private string GetDebuggerDisplay() => ToString();
}
