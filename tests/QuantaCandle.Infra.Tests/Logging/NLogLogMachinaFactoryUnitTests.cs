using Microsoft.Extensions.Logging;
using Moq;
using QuantaCandle.Infra.Logging;

namespace QuantaCandle.Infra.Tests.Logging;

public class NLogLogMachinaFactoryUnitTests
{
    [Fact]
    public void Constructor_WithValidLoggerFactory_CreatesInstance()
    {
        // Arrange
        var mockLoggerFactory = new Mock<ILoggerFactory>();

        // Act
        var factory = new NLogLogMachinaFactory(mockLoggerFactory.Object);

        // Assert
        Assert.NotNull(factory);
    }

    [Fact]
    public void Create_ReturnsNLogLogMachina()
    {
        // Arrange
        var mockLoggerFactory = new Mock<ILoggerFactory>();
        var factory = new NLogLogMachinaFactory(mockLoggerFactory.Object);

        // Act
        var machina = factory.Create<TestClass>();

        // Assert
        Assert.NotNull(machina);
        Assert.IsType<NLogLogMachina<TestClass>>(machina);
    }

    [Fact]
    public void Create_WithDifferentTypes_ReturnsCorrectMachinas()
    {
        // Arrange
        var mockLoggerFactory = new Mock<ILoggerFactory>();
        var factory = new NLogLogMachinaFactory(mockLoggerFactory.Object);

        // Act
        var machinaA = factory.Create<TypeA>();
        var machinaB = factory.Create<TypeB>();

        // Assert
        Assert.NotNull(machinaA);
        Assert.NotNull(machinaB);
        Assert.IsType<NLogLogMachina<TypeA>>(machinaA);
        Assert.IsType<NLogLogMachina<TypeB>>(machinaB);
    }

    [Fact]
    public void ImplementsILogMachinaFactory_Interface()
    {
        // Arrange
        var mockLoggerFactory = new Mock<ILoggerFactory>();

        // Act
        var factory = new NLogLogMachinaFactory(mockLoggerFactory.Object);

        // Assert
        Assert.IsAssignableFrom<ILogMachinaFactory>(factory);
    }

    [Fact]
    public void Create_MultipleCallsWithSameType_EachCallReturnsNewInstance()
    {
        // Arrange
        var mockLoggerFactory = new Mock<ILoggerFactory>();
        var factory = new NLogLogMachinaFactory(mockLoggerFactory.Object);

        // Act
        var machina1 = factory.Create<TestClass>();
        var machina2 = factory.Create<TestClass>();

        // Assert
        Assert.NotNull(machina1);
        Assert.NotNull(machina2);
        Assert.NotSame(machina1, machina2);
    }

    [Fact]
    public void Create_ReturnsILogMachina_Interface()
    {
        // Arrange
        var mockLoggerFactory = new Mock<ILoggerFactory>();
        var factory = new NLogLogMachinaFactory(mockLoggerFactory.Object);

        // Act
        var machina = factory.Create<TestClass>();

        // Assert
        Assert.IsAssignableFrom<ILogMachina<TestClass>>(machina);
    }

    // Test classes for generic type parameter
    private class TestClass { }
    private class TypeA { }
    private class TypeB { }
}
