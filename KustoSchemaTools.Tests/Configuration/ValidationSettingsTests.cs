using KustoSchemaTools.Configuration;
using Xunit;

namespace KustoSchemaTools.Tests.Configuration
{
    public class ValidationSettingsTests
    {
        [Fact]
        public void ValidationSettings_DefaultConstructor_HasValidationDisabled()
        {
            // Arrange & Act
            var settings = new ValidationSettings();

            // Assert
            Assert.False(settings.EnableColumnOrderValidation);
        }

        [Fact]
        public void WithColumnOrderValidation_ReturnsSettingsWithValidationEnabled()
        {
            // Arrange & Act
            var settings = ValidationSettings.WithColumnOrderValidation();

            // Assert
            Assert.True(settings.EnableColumnOrderValidation);
        }

        [Fact]
        public void FromEnvironment_WhenEnvironmentVariableNotSet_ReturnsDefaultSettings()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);

            // Act
            var settings = ValidationSettings.FromEnvironment();

            // Assert
            Assert.False(settings.EnableColumnOrderValidation);
        }

        [Fact]
        public void FromEnvironment_WhenEnvironmentVariableSetToTrue_EnablesValidation()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", "true");

            try
            {
                // Act
                var settings = ValidationSettings.FromEnvironment();

                // Assert
                Assert.True(settings.EnableColumnOrderValidation);
            }
            finally
            {
                // Cleanup
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Fact]
        public void FromEnvironment_WhenEnvironmentVariableSetToFalse_KeepsValidationDisabled()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", "false");

            try
            {
                // Act
                var settings = ValidationSettings.FromEnvironment();

                // Assert
                Assert.False(settings.EnableColumnOrderValidation);
            }
            finally
            {
                // Cleanup
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Theory]
        [InlineData("True")]
        [InlineData("TRUE")]
        [InlineData("1")]
        public void FromEnvironment_WhenEnvironmentVariableSetToTruthyValues_EnablesValidation(string value)
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", value);

            try
            {
                // Act
                var settings = ValidationSettings.FromEnvironment();

                // Assert
                Assert.True(settings.EnableColumnOrderValidation);
            }
            finally
            {
                // Cleanup
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Theory]
        [InlineData("False")]
        [InlineData("FALSE")]
        [InlineData("0")]
        [InlineData("invalid")]
        [InlineData("")]
        public void FromEnvironment_WhenEnvironmentVariableSetToFalsyOrInvalidValues_KeepsValidationDisabled(string value)
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", value);

            try
            {
                // Act
                var settings = ValidationSettings.FromEnvironment();

                // Assert
                Assert.False(settings.EnableColumnOrderValidation);
            }
            finally
            {
                // Cleanup
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }
    }
}
