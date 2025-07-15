using KustoSchemaTools.Model;
using KustoSchemaTools.Validation;
using Microsoft.Extensions.Logging;
using Moq;

namespace KustoSchemaTools.Tests.Validation
{
    public class MockAADObjectValidatorTests
    {
        private readonly Mock<ILogger<MockAADObjectValidator>> _mockLogger;
        private readonly MockAADObjectValidator _validator;

        public MockAADObjectValidatorTests()
        {
            _mockLogger = new Mock<ILogger<MockAADObjectValidator>>();
            _validator = new MockAADObjectValidator(_mockLogger.Object);
        }

        [Fact]
        public async Task ValidateAADObjectAsync_WithValidUser_ReturnsSuccess()
        {
            // Arrange
            var aadObject = new AADObject { Id = "aaduser=test@contoso.com", Name = "Test User" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.True(result.IsValid);
            Assert.Equal(aadObject.Id, result.Id);
            Assert.Equal(AADObjectType.User, result.Type);
        }

        [Fact]
        public async Task ValidateAADObjectAsync_WithValidGroup_ReturnsSuccess()
        {
            // Arrange
            var aadObject = new AADObject { Id = "aadgroup=test-group@contoso.com", Name = "Test Group" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.True(result.IsValid);
            Assert.Equal(aadObject.Id, result.Id);
            Assert.Equal(AADObjectType.Group, result.Type);
        }

        [Fact]
        public async Task ValidateAADObjectAsync_WithValidApp_ReturnsSuccess()
        {
            // Arrange
            var aadObject = new AADObject { Id = "aadapp=12345678-1234-1234-1234-123456789012", Name = "Test App" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.True(result.IsValid);
            Assert.Equal(aadObject.Id, result.Id);
            Assert.Equal(AADObjectType.Application, result.Type);
        }

        [Fact]
        public async Task ValidateAADObjectsAsync_WithMultipleObjects_ReturnsAllValid()
        {
            // Arrange
            var aadObjects = new[]
            {
                new AADObject { Id = "aaduser=user1@contoso.com", Name = "User 1" },
                new AADObject { Id = "aadgroup=group1@contoso.com", Name = "Group 1" },
                new AADObject { Id = "aadapp=12345678-1234-1234-1234-123456789012", Name = "App 1" }
            };

            // Act
            var results = await _validator.ValidateAADObjectsAsync(aadObjects);

            // Assert
            Assert.Equal(3, results.Count());
            Assert.All(results, result => Assert.True(result.IsValid));
        }

        [Fact]
        public async Task ValidateAADObjectsAsync_WithEmptyCollection_ReturnsEmptyResults()
        {
            // Arrange
            var aadObjects = Array.Empty<AADObject>();

            // Act
            var results = await _validator.ValidateAADObjectsAsync(aadObjects);

            // Assert
            Assert.Empty(results);
        }

        [Theory]
        [InlineData("")]
        [InlineData("   ")]
        public async Task ValidateAADObjectAsync_WithInvalidId_ReturnsValidButUnknownType(string invalidId)
        {
            // Arrange
            var aadObject = new AADObject { Id = invalidId, Name = "Test" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.True(result.IsValid); // Mock validator always returns valid
            Assert.Equal(invalidId, result.Id);
            Assert.Equal(AADObjectType.Unknown, result.Type);
        }

        [Fact]
        public async Task ValidateAADObjectAsync_WithNullObject_HandlesGracefully()
        {
            // Act
            var result = await _validator.ValidateAADObjectAsync(null!);

            // Assert
            Assert.True(result.IsValid); // Mock validator always returns valid
            Assert.Equal("unknown", result.Id);
        }

        [Fact]
        public async Task ValidateAADObjectsAsync_WithNullCollection_ThrowsArgumentNullException()
        {
            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(() => _validator.ValidateAADObjectsAsync(null!));
        }
    }
}
