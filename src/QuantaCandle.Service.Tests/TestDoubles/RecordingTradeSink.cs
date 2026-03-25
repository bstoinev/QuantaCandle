using QuantaCandle.Core.Trading;

namespace QuantaCandle.Service.Tests.TestDoubles;

public sealed class RecordingTradeSink : ITradeSink
{
    private readonly List<IReadOnlyList<TradeInfo>> _appends = new List<IReadOnlyList<TradeInfo>>();

    public IReadOnlyList<IReadOnlyList<TradeInfo>> Appends => _appends;

    public ValueTask<TradeAppendResult> Append(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
    {
        _appends.Add(trades);
        return ValueTask.FromResult(new TradeAppendResult(InsertedCount: trades.Count, DuplicateCount: 0));
    }
}

