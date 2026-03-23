using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;

using QuantaCandle.Core.Logging;

namespace QuantaCandle.Service.Tests.TestDoubles;

public sealed class TestLogMachinaFactory : ILogMachinaFactory
{
    public ILogMachina<T> Create<T>() where T : class
    {
        return new TestLogMachina<T>();
    }

    private sealed class TestLogMachina<T> : ILogMachina<T> where T : class
    {
        public void Debug(string message) => System.Diagnostics.Debug.Print($"{DateTime.Now:f} [DBG] {message}");
        public void Error(string message) => System.Diagnostics.Debug.Print($"{DateTime.Now:f} [ERR] {message}");
        public void Error(Exception ex) => System.Diagnostics.Debug.Print($"{DateTime.Now:f} [ERR] {ex}");
        public void Info(string message) => System.Diagnostics.Debug.Print($"{DateTime.Now:f} [INF] {message}");
        public void Trace(string message) => System.Diagnostics.Debug.Print($"{DateTime.Now:f} [TRC] {message}");
        public void Warn(string message) => System.Diagnostics.Debug.Print($"{DateTime.Now:f} [WRN] {message}");
    }
}

