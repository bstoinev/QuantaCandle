using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using QuantaCandle.Core.Trading;

namespace QuantaCandle.Service.Stubs;

public sealed class TradeSinkNull : ITradeSink
{
    public ValueTask<TradeAppendResult> AppendAsync(IReadOnlyList<TradeInfo> trades, CancellationToken cancellationToken)
    {
        TradeAppendResult result = new TradeAppendResult(InsertedCount: trades.Count, DuplicateCount: 0);
        return ValueTask.FromResult(result);
    }
}
