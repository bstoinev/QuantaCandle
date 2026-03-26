using LogMachina;
using LogMachina.NLog;

using SimpleInjector;

namespace LogMachina.SimpleInjector;

/// <summary>
/// Registers LogMachina services into a Simple Injector container.
/// </summary>
public static class LogMachinaSimpleInjectorExtensions
{
    /// <summary>
    /// Adds the LogMachina factory and typed logger registrations.
    /// </summary>
    public static void AddLogMachina(this Container container)
    {
        container.RegisterSingleton<ILogMachinaFactory, NLogLogMachinaFactory>();
        container.Register(typeof(ILogMachina<>), typeof(NLogLogMachina<>), Lifestyle.Singleton);
    }
}
