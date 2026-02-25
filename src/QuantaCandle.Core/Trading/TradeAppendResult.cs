namespace QuantaCandle.Core.Trading;

public readonly record struct TradeAppendResult(int InsertedCount, int DuplicateCount)
{
    public int TotalCount
    {
        get
        {
            return InsertedCount + DuplicateCount;
        }
    }
}
