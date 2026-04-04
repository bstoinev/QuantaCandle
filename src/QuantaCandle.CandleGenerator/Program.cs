using QuantaCandle.Infra.Generation;
using QuantaCandle.Infra.Storage;

return await Run(args).ConfigureAwait(false);

static async Task<int> Run(string[] args)
{
    var app = new CandleGeneratorApplication(new TradeToCandleGenerationRunner(), new LocalFileTradeGapScanner());
    var result = await app.Run(args, Console.Out, Console.Error, CancellationToken.None).ConfigureAwait(false);
    return result;
}
