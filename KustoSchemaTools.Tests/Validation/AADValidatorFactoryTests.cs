using KustoSchemaTools.Validation;
using Microsoft.Extensions.Logging;
using Moq;

namespace KustoSchemaTools.Tests.Validation
{
    public class AADValidatorFactoryTests
    {
        [Fact]
        public void CreateMockValidator_WithLogger_ReturnsValidMockValidator()
        {
            // Arrange
            var mockLogger = new Mock<ILogger<MockAADObjectValidator>>();

            // Act
            var validator = AADValidatorFactory.CreateMockValidator(mockLogger.Object);

            // Assert
            Assert.NotNull(validator);
            Assert.IsType<MockAADObjectValidator>(validator);
        }

        [Fact]
        public void CreateGraphValidatorFromEnvironment_WithLogger_ReturnsValidGraphValidator()
        {
            // Arrange
            var mockLogger = new Mock<ILogger<GraphAADObjectValidator>>();

            // Act
            var validator = AADValidatorFactory.CreateGraphValidatorFromEnvironment(mockLogger.Object);

            // Assert
            Assert.NotNull(validator);
            Assert.IsType<GraphAADObjectValidator>(validator);
        }
    }
}
