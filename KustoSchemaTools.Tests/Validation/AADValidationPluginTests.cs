using KustoSchemaTools.Model;
using KustoSchemaTools.Validation;
using Microsoft.Extensions.Logging;
using Moq;

namespace KustoSchemaTools.Tests.Validation
{
    public class AADValidationPluginTests
    {
        private readonly Mock<IAADObjectValidator> _mockValidator;
        private readonly Mock<ILogger<AADValidationPlugin>> _mockLogger;
        private readonly AADValidationPlugin _plugin;

        public AADValidationPluginTests()
        {
            _mockValidator = new Mock<IAADObjectValidator>();
            _mockLogger = new Mock<ILogger<AADValidationPlugin>>();
            _plugin = new AADValidationPlugin(_mockValidator.Object, _mockLogger.Object);
        }

        [Fact]
        public async Task OnLoad_WithDatabaseContainingAADObjects_ValidatesAllObjects()
        {
            // Arrange
            var database = CreateTestDatabase();
            var validationResults = CreateSuccessfulValidationResults();
            
            _mockValidator.Setup(v => v.ValidateAADObjectsAsync(It.IsAny<IEnumerable<AADObject>>()))
                         .ReturnsAsync(validationResults);

            // Act
            await _plugin.OnLoad(database, "/test/path");

            // Assert
            _mockValidator.Verify(v => v.ValidateAADObjectsAsync(It.IsAny<IEnumerable<AADObject>>()), Times.Once);
        }

        [Fact]
        public async Task OnLoad_WithEmptyDatabase_DoesNotCallValidator()
        {
            // Arrange
            var database = new Database { Name = "EmptyDatabase" };

            // Act
            await _plugin.OnLoad(database, "/test/path");

            // Assert
            _mockValidator.Verify(v => v.ValidateAADObjectsAsync(It.IsAny<IEnumerable<AADObject>>()), Times.Never);
        }

        [Fact]
        public async Task OnWrite_DoesNothing()
        {
            // Arrange
            var database = new Database { Name = "TestDatabase" };

            // Act & Assert (should not throw)
            await _plugin.OnWrite(database, "/test/path");
        }

        [Fact]
        public void Constructor_WithoutDependencies_CreatesPluginWithDefaultValidator()
        {
            // Act
            var plugin = new AADValidationPlugin();

            // Assert
            Assert.NotNull(plugin);
        }

        private Database CreateTestDatabase()
        {
            return new Database
            {
                Name = "TestDatabase",
                Admins = new List<AADObject>
                {
                    new AADObject { Id = "aaduser=admin@contoso.com", Name = "Admin User" }
                },
                Users = new List<AADObject>
                {
                    new AADObject { Id = "aadgroup=users@contoso.com", Name = "Users Group" }
                },
                Viewers = new List<AADObject>
                {
                    new AADObject { Id = "aadgroup=viewers@contoso.com", Name = "Viewers Group" }
                },
                Ingestors = new List<AADObject>
                {
                    new AADObject { Id = "aadapp=12345678-1234-1234-1234-123456789012", Name = "Ingestion App" }
                },
                Monitors = new List<AADObject>
                {
                    new AADObject { Id = "aadgroup=monitors@contoso.com", Name = "Monitors Group" }
                },
                UnrestrictedViewers = new List<AADObject>
                {
                    new AADObject { Id = "aadgroup=unrestricted@contoso.com", Name = "Unrestricted Viewers" }
                }
            };
        }

        private List<AADValidationResult> CreateSuccessfulValidationResults()
        {
            return new List<AADValidationResult>
            {
                new AADValidationResult { Id = "aaduser=admin@contoso.com", IsValid = true },
                new AADValidationResult { Id = "aadgroup=users@contoso.com", IsValid = true },
                new AADValidationResult { Id = "aadgroup=viewers@contoso.com", IsValid = true },
                new AADValidationResult { Id = "aadapp=12345678-1234-1234-1234-123456789012", IsValid = true },
                new AADValidationResult { Id = "aadgroup=monitors@contoso.com", IsValid = true },
                new AADValidationResult { Id = "aadgroup=unrestricted@contoso.com", IsValid = true }
            };
        }
    }
}
