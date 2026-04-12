using System.Globalization;

using QuantaCandle.Core.Trading;

namespace QuantaCandle.CLI;

/// <summary>
/// Writes trade gap download progress as a single-line pseudo-graphic indicator.
/// </summary>
internal sealed class TextWriterTradeGapProgressReporter(TextWriter outputWriter, string prefix) : ITradeGapProgressReporter
{
    private const int BarWidth = 24;

    private readonly TextWriter _outputWriter = outputWriter ?? throw new ArgumentNullException(nameof(outputWriter));
    private readonly string _prefix = prefix ?? throw new ArgumentNullException(nameof(prefix));
    private int _lastRenderedLength;

    /// <summary>
    /// Writes one formatted progress update.
    /// </summary>
    public async ValueTask Report(TradeGapProgressUpdate update, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var renderedLine = Render(update);
        var clearPadding = _lastRenderedLength > renderedLine.Length
            ? new string(' ', _lastRenderedLength - renderedLine.Length)
            : string.Empty;
        _lastRenderedLength = renderedLine.Length;

        await _outputWriter.WriteAsync("\r").ConfigureAwait(false);
        await _outputWriter.WriteAsync(renderedLine).ConfigureAwait(false);
        if (clearPadding.Length > 0)
        {
            await _outputWriter.WriteAsync(clearPadding).ConfigureAwait(false);
        }

        if (update.IsCompleted)
        {
            await _outputWriter.WriteLineAsync().ConfigureAwait(false);
            _lastRenderedLength = 0;
        }
    }

    /// <summary>
    /// Renders one pseudo-graphic progress line.
    /// </summary>
    private string Render(TradeGapProgressUpdate update)
    {
        var completedRatio = update.TotalTradeCount == 0
            ? 1m
            : Math.Clamp((decimal)update.CompletedTradeCount / update.TotalTradeCount, 0m, 1m);
        var filledWidth = (int)Math.Round(completedRatio * BarWidth, MidpointRounding.AwayFromZero);
        var bar = "[" + new string('#', filledWidth) + new string('-', BarWidth - filledWidth) + "]";
        var percent = (completedRatio * 100m).ToString("0", CultureInfo.InvariantCulture).PadLeft(3);
        var tradeCount = $"{update.CompletedTradeCount}/{update.TotalTradeCount}";
        var result = $"{_prefix}{bar} {percent}% trades {tradeCount} pages {update.DownloadedPageCount} {update.Stage}";
        return result;
    }
}
