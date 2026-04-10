using LogMachina;
using LogMachina.SimpleInjector;

using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance;
using QuantaCandle.Infra.Generation;
using QuantaCandle.Infra.Storage;

using SimpleInjector;

return await Run(args).ConfigureAwait(false);

static async Task<int> Run(string[] args)
{
    using var container = new Container();
    container.AddLogMachina(c => c.WithNLog(Lifestyle.Singleton));
    container.RegisterSingleton(() => new HttpClient());
    container.RegisterSingleton<ITradeGapFetchClient, BinanceTradeGapFetchClient>();
    container.RegisterSingleton<ITradeGapHealer>(
        () => new LocalFileTradeGapHealer(
            container.GetInstance<ITradeGapFetchClient>(),
            container.GetInstance<ILogMachina<LocalFileTradeGapHealer>>()));

    var app = new CliApplication(
        new TradeToCandleGenerationRunner(),
        new LocalFileTradeGapScanner(),
        container.GetInstance<ITradeGapHealer>());
    var result = await app.Run(args, Console.Out, Console.Error, CancellationToken.None).ConfigureAwait(false);
    return result;
}
