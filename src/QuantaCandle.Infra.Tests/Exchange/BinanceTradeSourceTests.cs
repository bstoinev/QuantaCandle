using System.Threading.Channels;

using LogMachina;

using Moq;

using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance;

namespace QuantaCandle.Infra.Tests.Exchange;

/// <summary>
/// Verifies Binance live trade source buffering and cancellation behavior.
/// </summary>
public sealed class BinanceTradeSourceTests
{
    [Fact]
    public async Task GetLiveTradesBlocksProducerAfterOneBufferedTrade()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var producerReachedThirdWrite = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var producerCompletedSecondWrite = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var producerCompletedThirdWrite = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = CreateSource(async (_, writer, cancellationToken) =>
        {
            await writer.WriteAsync(CreateTrade("1"), cancellationToken).ConfigureAwait(false);
            await writer.WriteAsync(CreateTrade("2"), cancellationToken).ConfigureAwait(false);
            producerCompletedSecondWrite.TrySetResult();

            producerReachedThirdWrite.TrySetResult();
            await writer.WriteAsync(CreateTrade("3"), cancellationToken).ConfigureAwait(false);
            producerCompletedThirdWrite.TrySetResult();
            writer.TryComplete();
        });
        await using var enumerator = source.GetLiveTrades(Instrument.Parse("BTC-USDT"), cancellationToken).GetAsyncEnumerator(cancellationToken);

        Assert.True(await enumerator.MoveNextAsync().AsTask());
        Assert.Equal("1", enumerator.Current.Key.TradeId);

        await producerCompletedSecondWrite.Task;
        await producerReachedThirdWrite.Task;

        Assert.False(producerCompletedThirdWrite.Task.IsCompleted);

        Assert.True(await enumerator.MoveNextAsync().AsTask());
        Assert.Equal("2", enumerator.Current.Key.TradeId);

        await producerCompletedThirdWrite.Task;

        Assert.True(await enumerator.MoveNextAsync().AsTask());
        Assert.Equal("3", enumerator.Current.Key.TradeId);
        Assert.False(await enumerator.MoveNextAsync().AsTask());
    }

    [Fact]
    public async Task GetLiveTradesPropagatesCancellationToProducer()
    {
        var producerObservedCancellation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var source = CreateSource(async (_, writer, cancellationToken) =>
        {
            await writer.WriteAsync(CreateTrade("1"), cancellationToken).ConfigureAwait(false);

            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                producerObservedCancellation.TrySetResult();
            }
            finally
            {
                writer.TryComplete();
            }
        });
        await using var enumerator = source.GetLiveTrades(Instrument.Parse("BTC-USDT"), cancellationTokenSource.Token).GetAsyncEnumerator(cancellationTokenSource.Token);

        Assert.True(await enumerator.MoveNextAsync().AsTask());
        Assert.Equal("1", enumerator.Current.Key.TradeId);

        cancellationTokenSource.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => enumerator.MoveNextAsync().AsTask());
        await producerObservedCancellation.Task;
    }

    private static BinanceTradeSource CreateSource(Func<Instrument, ChannelWriter<TradeInfo>, CancellationToken, Task> tradeProducer)
    {
        var result = new BinanceTradeSource(
            new BinanceTradeSourceOptions(
                BaseWebSocketUrl: "wss://unit-test.invalid",
                InitialReconnectDelay: TimeSpan.FromMilliseconds(10),
                MaxReconnectDelay: TimeSpan.FromMilliseconds(20),
                ReceiveBufferSize: 1024),
            CreateLogMoq().Object,
            tradeProducer);
        return result;
    }

    private static Mock<ILogMachina<BinanceTradeSource>> CreateLogMoq()
    {
        var result = new Mock<ILogMachina<BinanceTradeSource>>();
        return result;
    }

    private static TradeInfo CreateTrade(string tradeId)
    {
        var key = new TradeKey(new ExchangeId("Binance"), Instrument.Parse("BTC-USDT"), tradeId);
        var result = new TradeInfo(key, new DateTimeOffset(2026, 3, 11, 12, 0, 0, TimeSpan.Zero), 50_000m, 0.001m, buyerIsMaker: false);
        return result;
    }
}
