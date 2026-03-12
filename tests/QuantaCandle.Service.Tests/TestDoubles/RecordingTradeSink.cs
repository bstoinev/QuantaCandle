using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Service.Tests.TestDoubles;

public sealed class RecordingTradeSink : ITradeSink
{
    private readonly List<IReadOnlyList<TradeInfo>> appends;

    public RecordingTradeSink()
    {
        appends = new List<IReadOnlyList<TradeInfo>>();
    }

    public IReadOnlyList<IReadOnlyList<TradeInfo>> Appends
    {
        get
        {
            return appends;
        }
    }

    public ValueTask<TradeAppendResult> AppendAsync(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
    {
        appends.Add(trades);
        return ValueTask.FromResult(new TradeAppendResult(InsertedCount: trades.Count, DuplicateCount: 0));
    }
}

