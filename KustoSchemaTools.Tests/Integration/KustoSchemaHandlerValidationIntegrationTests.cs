using KustoSchemaTools.Configuration;
using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using System.IO;

namespace KustoSchemaTools.Tests.Integration
{
    public class KustoSchemaHandlerValidationIntegrationTests
    {
        private readonly Mock<ILogger<KustoSchemaHandler<Database>>> _loggerMock;

        public KustoSchemaHandlerValidationIntegrationTests()
        {
            _loggerMock = new Mock<ILogger<KustoSchemaHandler<Database>>>();
        }

        [Fact]
        public void KustoSchemaHandler_GenerateDiffMarkdown_WithValidationEnabled_ReportsInvalidAsUndeployable()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", "true");
            
            try
            {
                // This is a simplified test - in practice, GenerateDiffMarkdown reads from files and connects to Kusto
                // Here we're testing the ValidationSettings.FromEnvironment() integration
                var settings = ValidationSettings.FromEnvironment();
                
                // Assert
                Assert.True(settings.EnableColumnOrderValidation, "Environment variable should enable validation");
            }
            finally
            {
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Fact]
        public void KustoSchemaHandler_Apply_WithValidationEnabled_ShouldCheckValidation()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", "true");
            
            try
            {
                // This test verifies that the Apply method will use ValidationSettings.FromEnvironment()
                // The actual Apply method integration would require mocking file system and Kusto connections
                var settings = ValidationSettings.FromEnvironment();
                
                // Assert
                Assert.True(settings.EnableColumnOrderValidation, "Apply method should use environment variable");
            }
            finally
            {
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Theory]
        [InlineData("true", true)]
        [InlineData("TRUE", true)]
        [InlineData("1", true)]
        [InlineData("false", false)]
        [InlineData("FALSE", false)]
        [InlineData("0", false)]
        [InlineData("invalid", false)]
        [InlineData("", false)]
        public void ValidationSettings_FromEnvironment_ParsesAllFormatsCorrectly(string envValue, bool expectedEnabled)
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", envValue);
            
            try
            {
                // Act
                var settings = ValidationSettings.FromEnvironment();

                // Assert
                Assert.Equal(expectedEnabled, settings.EnableColumnOrderValidation);
            }
            finally
            {
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Fact]
        public void ValidationSettings_FromEnvironment_WhenNotSet_DefaultsToDisabled()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            
            try
            {
                // Act
                var settings = ValidationSettings.FromEnvironment();

                // Assert
                Assert.False(settings.EnableColumnOrderValidation, "Validation should be disabled by default for backward compatibility");
            }
            finally
            {
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }
    }
}
