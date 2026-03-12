using System.Buffers;
using System.Globalization;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using QuantaCandle.Core.Logging;
using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance.Dtos;
using QuantaCandle.Exchange.Binance.Internal;

namespace QuantaCandle.Exchange.Binance;

public sealed class BinanceTradeSource : ITradeSource
{
    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions();

    private static readonly ExchangeId BinanceExchange = new ExchangeId("Binance");

    private readonly BinanceTradeSourceOptions options;
    private readonly ILogMachina<BinanceTradeSource> logMachina;

    public BinanceTradeSource(BinanceTradeSourceOptions options, ILogMachinaFactory logMachinaFactory)
    {
        this.options = options;
        logMachina = logMachinaFactory.Create<BinanceTradeSource>();
    }

    public ExchangeId Exchange
    {
        get
        {
            return BinanceExchange;
        }
    }

    public async IAsyncEnumerable<TradeInfo> GetLiveTradesAsync(
        Instrument symbol,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var logger = logMachina.GetLogger();

        Channel<TradeInfo> channel = Channel.CreateUnbounded<TradeInfo>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });

        Task producer = ProduceTradesAsync(symbol, channel.Writer, cancellationToken);

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
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Binance trade producer ended with error for {symbol}.", symbol);
            }
        }
    }

    public async IAsyncEnumerable<TradeInfo> GetBackfillTradesAsync(
        Instrument symbol,
        TradeWatermark? fromExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }

    private Uri BuildTradeStreamUri(Instrument instrument)
    {
        string streamSymbol = BinanceSymbol.ToStreamSymbol(instrument);
        string path = $"/ws/{streamSymbol}@trade";
        return new Uri($"{options.BaseWebSocketUrl}{path}");
    }

    private async IAsyncEnumerable<TradeInfo> ReadTradesAsync(
        ClientWebSocket ws,
        Instrument instrument,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var logger = logMachina.GetLogger();
        bool loggedFirstMessage = false;
        bool loggedFirstParseFailure = false;

        byte[] buffer = ArrayPool<byte>.Shared.Rent(options.ReceiveBufferSize);
        try
        {
            while (ws.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
            {
                string? json = await ReceiveTextMessageAsync(ws, buffer, cancellationToken).ConfigureAwait(false);
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
                    string snippet = json.Length > 300 ? json.Substring(0, 300) : json;
                    logger.LogInformation("First Binance message for {instrument}: {snippet}", instrument, snippet);
                }

                BinanceTradeMessageDto? dto = TryDeserializeTrade(json, out Exception? error);
                if (dto is null)
                {
                    if (!loggedFirstParseFailure)
                    {
                        loggedFirstParseFailure = true;
                        string snippet = json.Length > 300 ? json.Substring(0, 300) : json;
                        logger.LogWarning(error, "Failed to parse Binance trade message for {instrument}: {snippet}", instrument, snippet);
                    }

                    continue;
                }

                TradeInfo trade = Map(dto, instrument);
                yield return trade;
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
                    BinanceCombinedStreamEnvelopeDto<BinanceTradeMessageDto>? envelope =
                        JsonSerializer.Deserialize<BinanceCombinedStreamEnvelopeDto<BinanceTradeMessageDto>>(json, JsonOptions);
                    return envelope?.Data;
                }

                return JsonSerializer.Deserialize<BinanceTradeMessageDto>(json, JsonOptions);
            }
        }
        catch (Exception ex)
        {
            error = ex;
        }

        return null;
    }

    private TradeInfo Map(BinanceTradeMessageDto dto, Instrument instrument)
    {
        DateTimeOffset timestamp = DateTimeOffset.FromUnixTimeMilliseconds(dto.TradeTime);

        string tradeId = dto.TradeId.ToString(CultureInfo.InvariantCulture);
        string priceRaw = dto.Price ?? "0";
        string qtyRaw = dto.Quantity ?? "0";

        decimal price = decimal.Parse(priceRaw, NumberStyles.Float, CultureInfo.InvariantCulture);
        decimal quantity = decimal.Parse(qtyRaw, NumberStyles.Float, CultureInfo.InvariantCulture);

        TradeKey key = new TradeKey(BinanceExchange, instrument, tradeId);
        return new TradeInfo(key, timestamp, price, quantity);
    }

    private static async Task<string?> ReceiveTextMessageAsync(ClientWebSocket ws, byte[] buffer, CancellationToken cancellationToken)
    {
        using MemoryStream stream = new MemoryStream();

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

    private async Task ProduceTradesAsync(Instrument instrument, ChannelWriter<TradeInfo> writer, CancellationToken cancellationToken)
    {
        var logger = logMachina.GetLogger();
        TimeSpan delay = options.InitialReconnectDelay;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                using ClientWebSocket ws = new ClientWebSocket();
                ws.Options.KeepAliveInterval = TimeSpan.FromSeconds(20);
                ws.Options.Proxy = null;

                Uri uri = BuildTradeStreamUri(instrument);

                try
                {
                    logger.LogInformation("Connecting to Binance trade stream: {uri}", uri);
                    await ws.ConnectAsync(uri, cancellationToken).ConfigureAwait(false);
                    logger.LogInformation("Connected to Binance trade stream: {uri}", uri);

                    delay = options.InitialReconnectDelay;

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
                    logger.LogWarning(ex, "Binance trade stream error for {instrument}; reconnecting in {delay}.", instrument, delay);
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    delay = ExponentialBackoff.NextDelay(delay, options.MaxReconnectDelay);
                }
            }
        }
        finally
        {
            writer.TryComplete();
        }
    }
}
