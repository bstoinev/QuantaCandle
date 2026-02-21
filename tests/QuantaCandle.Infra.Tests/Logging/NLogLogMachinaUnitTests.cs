using Microsoft.Extensions.Logging;
using Moq;
using QuantaCandle.Infra.Logging;

namespace QuantaCandle.Infra.Tests.Logging;

public class NLogLogMachinaUnitTests
{
    private class SampleTestClass { }
    private class AnotherTestClass { }

    [Fact]
    public void Constructor_WithValidLoggerFactory_CreatesInstance()
    {
        // Arrange
        var mockLoggerFactory = new Mock<ILoggerFactory>();

        // Act
        var machina = new NLogLogMachina<SampleTestClass>(mockLoggerFactory.Object);

        // Assert
        Assert.NotNull(machina);
    }

    [Fact]
    public void GetLogger_ReturnsLoggerInstance()
    {
        // Arrange
        var mockLoggerFactory = new Mock<ILoggerFactory>();

        var machina = new NLogLogMachina<SampleTestClass>(mockLoggerFactory.Object);

        // Act
        var logger = machina.GetLogger();

        // Assert
        Assert.NotNull(logger);
        Assert.IsAssignableFrom<ILogger<SampleTestClass>>(logger);
    }

    [Fact]
    public void GetLogger_MultipleCallsReturnSameInstance()
    {
        // Arrange
        var mockLoggerFactory = new Mock<ILoggerFactory>();
        var machina = new NLogLogMachina<SampleTestClass>(mockLoggerFactory.Object);

        // Act
        var logger1 = machina.GetLogger();
        var logger2 = machina.GetLogger();

        // Assert
        Assert.NotNull(logger1);
        Assert.NotNull(logger2);
        Assert.Same(logger1, logger2);
    }

    [Fact]
    public void ImplementsILogMachina()
    {
        // Arrange
        var mockLoggerFactory = new Mock<ILoggerFactory>();

        // Act
        var machina = new NLogLogMachina<SampleTestClass>(mockLoggerFactory.Object);

        // Assert
        Assert.IsAssignableFrom<ILogMachina<SampleTestClass>>(machina);
    }

    [Fact]
    public void ImplementsLogMachinaBase()
    {
        // Arrange
        var mockLoggerFactory = new Mock<ILoggerFactory>();

        // Act
        var machina = new NLogLogMachina<AnotherTestClass>(mockLoggerFactory.Object);

        // Assert
        Assert.IsAssignableFrom<LogMachinaBase<AnotherTestClass>>(machina);
    }
}
