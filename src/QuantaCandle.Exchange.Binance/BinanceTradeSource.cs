using System.Buffers;
using System.Globalization;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;

using LogMachina;

using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance.Internal;

namespace QuantaCandle.Exchange.Binance;

/// <summary>
/// Streams Binance trades while keeping source-side buffering tightly bounded.
/// </summary>
public sealed class BinanceTradeSource : ITradeSource
{
    private const int LiveTradeBufferCapacity = 1;
    private static readonly JsonSerializerOptions _jsonOptions = new();
    private static readonly ExchangeId _binanceExchange = new("Binance");
    private readonly ILogMachina<BinanceTradeSource> _log;
    private readonly BinanceTradeSourceOptions _options;
    private readonly Func<Instrument, ChannelWriter<TradeInfo>, CancellationToken, Task> _tradeProducer;

    /// <summary>
    /// Initializes the Binance live trade source.
    /// </summary>
    public BinanceTradeSource(BinanceTradeSourceOptions options, ILogMachina<BinanceTradeSource> log)
        : this(options, log, null)
    {
    }

    /// <summary>
    /// Initializes the Binance live trade source with a test producer override.
    /// </summary>
    internal BinanceTradeSource(
        BinanceTradeSourceOptions options,
        ILogMachina<BinanceTradeSource> log,
        Func<Instrument, ChannelWriter<TradeInfo>, CancellationToken, Task>? tradeProducer)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _tradeProducer = tradeProducer ?? ProduceTradesAsync;
    }

    public ExchangeId Exchange => _binanceExchange;

    public async IAsyncEnumerable<TradeInfo> GetLiveTrades(Instrument symbol, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = CreateLiveTradeChannel();
        var producer = _tradeProducer(symbol, channel.Writer, cancellationToken);

        try
        {
            await foreach (TradeInfo trade in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return trade;
            }
        }
        finally
        {
            channel.Writer.TryComplete();
            try
            {
                await producer.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
            catch (Exception ex)
            {
                _log.Error(ex);
                _log.Warn($"Binance trade producer ended with error for {symbol}.");
            }
        }
    }

    public async IAsyncEnumerable<TradeInfo> GetBackfillTrades(
        Instrument symbol,
        TradeWatermark? fromExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }

    private Uri BuildTradeStreamUri(Instrument instrument)
    {
        var streamSymbol = BinanceSymbol.ToStreamSymbol(instrument);
        var path = $"/ws/{streamSymbol}@trade";
        return new Uri($"{_options.BaseWebSocketUrl}{path}");
    }

    private async IAsyncEnumerable<TradeInfo> ReadTradesAsync(
        ClientWebSocket ws,
        Instrument instrument,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var loggedFirstMessage = false;
        var loggedFirstParseFailure = false;
        var buffer = ArrayPool<byte>.Shared.Rent(_options.ReceiveBufferSize);

        try
        {
            while (ws.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
            {
                var json = await ReceiveTextMessageAsync(ws, buffer, cancellationToken).ConfigureAwait(false);

                if (json is null)
                {
                    yield break;
                }

                if (string.IsNullOrWhiteSpace(json))
                {
                    continue;
                }

                if (!loggedFirstMessage)
                {
                    loggedFirstMessage = true;
                    var snippet = json.Length > 300 ? json[..300] : json;
                    _log.Info($"First Binance message for {instrument}: {snippet}");
                }

                var dto = TryDeserializeTrade(json, out Exception? error);

                if (dto is null)
                {
                    if (!loggedFirstParseFailure)
                    {
                        loggedFirstParseFailure = true;
                        string snippet = json.Length > 300 ? $"{json.Substring(0, 300)}..." : json;
                        _log.Warn($"Failed to parse Binance trade message for {instrument}: {snippet}");
                    }
                }
                else
                {
                    var trade = Map(dto, instrument);
                    yield return trade;
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static BinanceTradeMessageDto? TryDeserializeTrade(string json, out Exception? error)
    {
        error = null;
        try
        {
            if (json.Length > 0 && json[0] == '{')
            {
                if (json.Contains("\"data\"", StringComparison.Ordinal))
                {
                    var envelope = JsonSerializer.Deserialize<BinanceCombinedStreamEnvelopeDto<BinanceTradeMessageDto>>(json, _jsonOptions);
                    return envelope?.Data;
                }

                return JsonSerializer.Deserialize<BinanceTradeMessageDto>(json, _jsonOptions);
            }
        }
        catch (Exception ex)
        {
            error = ex;
        }

        return null;
    }

    private static TradeInfo Map(BinanceTradeMessageDto dto, Instrument instrument)
    {
        var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(dto.TradeTime);
        var tradeId = dto.TradeId.ToString(CultureInfo.InvariantCulture);
        var priceRaw = dto.Price ?? "0";
        var qtyRaw = dto.Quantity ?? "0";
        var price = decimal.Parse(priceRaw, NumberStyles.Float, CultureInfo.InvariantCulture);
        var quantity = decimal.Parse(qtyRaw, NumberStyles.Float, CultureInfo.InvariantCulture);
        var key = new TradeKey(_binanceExchange, instrument, tradeId);

        return new TradeInfo(key, timestamp, price, quantity, dto.BuyerIsMaker);
    }

    private static async Task<string?> ReceiveTextMessageAsync(ClientWebSocket ws, byte[] buffer, CancellationToken cancellationToken)
    {
        using var stream = new MemoryStream();

        while (true)
        {
            ValueWebSocketReceiveResult result = await ws.ReceiveAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                return null;
            }

            if (result.Count > 0)
            {
                stream.Write(buffer, 0, result.Count);
            }

            if (result.EndOfMessage)
            {
                break;
            }
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static Channel<TradeInfo> CreateLiveTradeChannel()
    {
        var options = new BoundedChannelOptions(LiveTradeBufferCapacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false,
        };

        var result = Channel.CreateBounded<TradeInfo>(options);
        return result;
    }

    private async Task ProduceTradesAsync(Instrument instrument, ChannelWriter<TradeInfo> writer, CancellationToken cancellationToken)
    {
        var delay = _options.InitialReconnectDelay;
        var uri = BuildTradeStreamUri(instrument);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                _log.Debug($"Connecting to Binance...");

                using var ws = new ClientWebSocket();
                ws.Options.KeepAliveInterval = TimeSpan.FromSeconds(20);
                ws.Options.Proxy = null;

                try
                {
                    await ws.ConnectAsync(uri, cancellationToken).ConfigureAwait(false);
                    _log.Info($"Connected to Binance trade stream: {uri}");

                    delay = _options.InitialReconnectDelay;

                    await foreach (TradeInfo trade in ReadTradesAsync(ws, instrument, cancellationToken).ConfigureAwait(false))
                    {
                        await writer.WriteAsync(trade, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _log.Error(ex);
                    _log.Warn($"Binance stream error: {ex.Message}. Attempting reconnect in {delay.TotalSeconds:N} seconds");

                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    delay = ExponentialBackoff.NextDelay(delay, _options.MaxReconnectDelay);
                }
            }
        }
        finally
        {
            writer.TryComplete();
        }
    }
}
