using QuantaCandle.CLI;

using SimpleInjector;

Container container = CompositionRoot.ConfigureServices();

try
{
    Console.WriteLine("Hello, World!");
}
finally
{
    await container.DisposeAsync().ConfigureAwait(false);
}
