using KustoSchemaTools.Validation;

namespace KustoSchemaTools.Tests.Validation
{
    public class AADValidationResultTests
    {
        [Fact]
        public void AADValidationResult_DefaultConstructor_InitializesCorrectly()
        {
            // Act
            var result = new AADValidationResult();

            // Assert
            Assert.Equal(string.Empty, result.Id);
            Assert.False(result.IsValid);
            Assert.Equal(string.Empty, result.ErrorMessage);
        }

        [Fact]
        public void AADValidationResult_WithValidResult_SetsCorrectly()
        {
            // Arrange
            var objectId = "aaduser=test@contoso.com";

            // Act
            var result = new AADValidationResult
            {
                Id = objectId,
                IsValid = true,
                ErrorMessage = string.Empty
            };

            // Assert
            Assert.Equal(objectId, result.Id);
            Assert.True(result.IsValid);
            Assert.Equal(string.Empty, result.ErrorMessage);
        }

        [Fact]
        public void AADValidationResult_WithInvalidResult_SetsCorrectly()
        {
            // Arrange
            var objectId = "aaduser=invalid@contoso.com";
            var errorMessage = "User not found";

            // Act
            var result = new AADValidationResult
            {
                Id = objectId,
                IsValid = false,
                ErrorMessage = errorMessage
            };

            // Assert
            Assert.Equal(objectId, result.Id);
            Assert.False(result.IsValid);
            Assert.Equal(errorMessage, result.ErrorMessage);
        }

        [Theory]
        [InlineData("aaduser=user@domain.com")]
        [InlineData("aadgroup=group@domain.com")]
        [InlineData("aadapp=12345678-1234-1234-1234-123456789012")]
        [InlineData("")]
        public void AADValidationResult_WithVariousObjectIds_HandlesCorrectly(string objectId)
        {
            // Act
            var result = new AADValidationResult { Id = objectId };

            // Assert
            Assert.Equal(objectId, result.Id);
        }

        [Fact]
        public void AADValidationResult_ToString_ContainsRelevantInformation()
        {
            // Arrange
            var result = new AADValidationResult
            {
                Id = "aaduser=test@contoso.com",
                IsValid = false,
                ErrorMessage = "User not found"
            };

            // Act
            var stringRepresentation = result.ToString();

            // Assert - Since there's no custom ToString(), it returns the class name
            Assert.Contains("AADValidation", stringRepresentation);
        }

        [Fact]
        public void AADValidationResult_IsValidTrue_WithoutErrorMessage()
        {
            // Act
            var result = new AADValidationResult
            {
                Id = "aaduser=valid@contoso.com",
                IsValid = true
            };

            // Assert
            Assert.True(result.IsValid);
            Assert.Equal(string.Empty, result.ErrorMessage);
        }

        [Fact]
        public void AADValidationResult_IsValidFalse_WithErrorMessage()
        {
            // Arrange
            var errorMessage = "Access denied";

            // Act
            var result = new AADValidationResult
            {
                Id = "aaduser=denied@contoso.com",
                IsValid = false,
                ErrorMessage = errorMessage
            };

            // Assert
            Assert.False(result.IsValid);
            Assert.Equal(errorMessage, result.ErrorMessage);
        }
    }
}
