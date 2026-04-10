using System.Runtime.CompilerServices;

using LogMachina.SimpleInjector;

using QuantaCandle.Core.Trading;
using QuantaCandle.Exchange.Binance;
using QuantaCandle.Infra.Storage;

using SimpleInjector;

[assembly: InternalsVisibleTo("QuantaCandle.CLI.Tests")]

namespace QuantaCandle.CLI;

internal class Program
{
    private static async Task<int> Main(string[] args) => await Run(args).ConfigureAwait(false);

    private static async Task<int> Run(string[] args)
    {
        using var container = new Container();
        container.AddLogMachina(c => c.WithNLog(Lifestyle.Singleton));
        container.RegisterSingleton(() => new HttpClient());
        container.RegisterSingleton<ITradeGapFetchClient, BinanceTradeGapFetchClient>();
        container.RegisterSingleton<ITradeGapScanner, LocalFileTradeGapScanner>();
        container.RegisterSingleton<ITradeGapHealer, LocalFileTradeGapHealer>();

        container.RegisterSingleton<IQuantaCandleRunner, QuantaCandleRunner>();
        container.RegisterSingleton<CliApplication>();


        var app = container.GetInstance<CliApplication>(); 
        var result = await app.Run(args, Console.Out, Console.Error, CancellationToken.None).ConfigureAwait(false);
        return result;
    }
}
