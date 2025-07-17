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
            var aadObject = new AADObject { Id = "aadapp=12345678-1234-1234-1234-123456789012;contoso.com", Name = "Test App" };

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
                new AADObject { Id = "aadapp=12345678-1234-1234-1234-123456789012;contoso.com", Name = "App 1" }
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
        public async Task ValidateAADObjectAsync_WithInvalidId_ReturnsInvalidAndUnknownType(string invalidId)
        {
            // Arrange
            var aadObject = new AADObject { Id = invalidId, Name = "Test" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.False(result.IsValid); // Enhanced mock validator validates format
            Assert.Equal(invalidId, result.Id);
            Assert.Equal(AADObjectType.Unknown, result.Type);
        }

        [Fact]
        public async Task ValidateAADObjectAsync_WithNullObject_HandlesGracefully()
        {
            // Act
            var result = await _validator.ValidateAADObjectAsync(null!);

            // Assert
            Assert.False(result.IsValid); // Enhanced mock validator validates format
            Assert.Equal("unknown", result.Id);
        }

        [Fact]
        public async Task ValidateAADObjectsAsync_WithNullCollection_ThrowsArgumentNullException()
        {
            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(() => _validator.ValidateAADObjectsAsync(null!));
        }

        #region AAD Principal Format Tests (Based on Microsoft Documentation)
        // Reference: https://learn.microsoft.com/en-us/kusto/management/reference-security-principals#referencing-microsoft-entra-principals-and-groups

        [Theory]
        [InlineData("aaduser=user@contoso.com", AADObjectType.User)]                                                    // User Implicit: aaduser=UPN
        [InlineData("aaduser=user@contoso.com;72f988bf-86f1-41af-91ab-2d7cd011db47", AADObjectType.User)]              // User Explicit (ID): aaduser=UPN;TenantId
        [InlineData("aaduser=user@contoso.com;contoso.com", AADObjectType.User)]                                        // User Explicit (Name): aaduser=UPN;TenantName
        [InlineData("aaduser=12345678-1234-1234-1234-123456789012;72f988bf-86f1-41af-91ab-2d7cd011db47", AADObjectType.User)]  // User Explicit (ID): aaduser=ObjectID;TenantId
        [InlineData("aaduser=12345678-1234-1234-1234-123456789012;contoso.com", AADObjectType.User)]                   // User Explicit (Name): aaduser=ObjectID;TenantName
        public async Task ValidateAADObjectAsync_WithValidUserFormats_ReturnsCorrectType(string aadId, AADObjectType expectedType)
        {
            // Arrange
            var aadObject = new AADObject { Id = aadId, Name = "Test User" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.True(result.IsValid);
            Assert.Equal(aadId, result.Id);
            Assert.Equal(expectedType, result.Type);
        }

        [Theory]
        [InlineData("aadgroup=group@contoso.com", AADObjectType.Group)]                                                 // Group Implicit: aadgroup=GroupEmailAddress
        [InlineData("aadgroup=My Group Name;72f988bf-86f1-41af-91ab-2d7cd011db47", AADObjectType.Group)]              // Group Explicit (ID): aadgroup=GroupDisplayName;TenantId
        [InlineData("aadgroup=My Group Name;contoso.com", AADObjectType.Group)]                                         // Group Explicit (Name): aadgroup=GroupDisplayName;TenantName
        [InlineData("aadgroup=12345678-1234-1234-1234-123456789012;72f988bf-86f1-41af-91ab-2d7cd011db47", AADObjectType.Group)]  // Group Explicit (ID): aadgroup=GroupObjectId;TenantId
        [InlineData("aadgroup=12345678-1234-1234-1234-123456789012;contoso.com", AADObjectType.Group)]                // Group Explicit (Name): aadgroup=GroupObjectId;TenantName
        public async Task ValidateAADObjectAsync_WithValidGroupFormats_ReturnsCorrectType(string aadId, AADObjectType expectedType)
        {
            // Arrange
            var aadObject = new AADObject { Id = aadId, Name = "Test Group" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.True(result.IsValid);
            Assert.Equal(aadId, result.Id);
            Assert.Equal(expectedType, result.Type);
        }

        [Theory]
        // Note: Apps have NO implicit format - must always specify tenant
        [InlineData("aadapp=My Application;72f988bf-86f1-41af-91ab-2d7cd011db47", AADObjectType.Application)]          // App Explicit (ID): aadapp=ApplicationDisplayName;TenantId
        [InlineData("aadapp=My Application;contoso.com", AADObjectType.Application)]                                    // App Explicit (Name): aadapp=ApplicationDisplayName;TenantName
        [InlineData("aadapp=12345678-1234-1234-1234-123456789012;72f988bf-86f1-41af-91ab-2d7cd011db47", AADObjectType.Application)]  // App Explicit (ID): aadapp=ApplicationId;TenantId
        [InlineData("aadapp=12345678-1234-1234-1234-123456789012;contoso.com", AADObjectType.Application)]            // App Explicit (Name): aadapp=ApplicationId;TenantName
        public async Task ValidateAADObjectAsync_WithValidAppFormats_ReturnsCorrectType(string aadId, AADObjectType expectedType)
        {
            // Arrange
            var aadObject = new AADObject { Id = aadId, Name = "Test Application" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.True(result.IsValid);
            Assert.Equal(aadId, result.Id);
            Assert.Equal(expectedType, result.Type);
        }

        [Theory]
        [InlineData("aadapp=12345678-1234-1234-1234-123456789012")]  // Invalid: App without tenant (implicit not allowed)
        public async Task ValidateAADObjectAsync_WithInvalidAppFormats_ReturnsInvalidType(string aadId)
        {
            // Arrange
            var aadObject = new AADObject { Id = aadId, Name = "Test Application" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.False(result.IsValid); // Enhanced mock validator validates format
            Assert.Equal(AADObjectType.Application, result.Type); // But type detection works for the prefix
        }

        [Theory]
        [InlineData("aaduser=user@invalid-domain.com", false)]
        [InlineData("aaduser=user@invalid-domain.com;72f988bf-86f1-41af-91ab-2d7cd011db47", false)]
        [InlineData("aadgroup=definitely-invalid-group@contoso.com", false)]
        [InlineData("aadgroup=definitely-invalid-group@contoso.com;72f988bf-86f1-41af-91ab-2d7cd011db47", false)]
        [InlineData("aaduser=valid@contoso.com", true)]
        [InlineData("aaduser=valid@contoso.com;72f988bf-86f1-41af-91ab-2d7cd011db47", true)]
        [InlineData("aadapp=MyApp;contoso.com", true)]  // Valid app format
        public async Task ValidateAADObjectAsync_WithInvalidPrincipals_ReturnsExpectedValidation(string aadId, bool expectedValid)
        {
            // Arrange
            var aadObject = new AADObject { Id = aadId, Name = "Test Object" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.Equal(expectedValid, result.IsValid);
            Assert.Equal(aadId, result.Id);
            if (!expectedValid)
            {
                Assert.Equal("Mock validation: Principal appears to be invalid or has invalid format", result.ErrorMessage);
            }
        }

        [Theory]
        [InlineData("user@contoso.com;72f988bf-86f1-41af-91ab-2d7cd011db47", AADObjectType.User)]
        [InlineData("My Group;contoso.com", AADObjectType.Group)]
        [InlineData("12345678-1234-1234-1234-123456789012;72f988bf-86f1-41af-91ab-2d7cd011db47", AADObjectType.Application)]
        public async Task ValidateAADObjectByIdAsync_WithTenantFormats_HandlesCorrectly(string idPart, AADObjectType expectedType)
        {
            // Act
            var result = await _validator.ValidateAADObjectByIdAsync(idPart, expectedType);

            // Assert
            Assert.True(result.IsValid);
            Assert.Equal(idPart, result.Id);
            Assert.Equal(expectedType, result.Type);
        }

        [Fact]
        public async Task ValidateAADObjectsAsync_WithMixedValidFormats_ValidatesAllCorrectly()
        {
            // Arrange - All valid formats according to Microsoft documentation
            var aadObjects = new[]
            {
                new AADObject { Id = "aaduser=user1@contoso.com", Name = "User 1 (UPN implicit)" },
                new AADObject { Id = "aaduser=user2@contoso.com;72f988bf-86f1-41af-91ab-2d7cd011db47", Name = "User 2 (UPN with tenant ID)" },
                new AADObject { Id = "aaduser=user3@contoso.com;contoso.com", Name = "User 3 (UPN with tenant name)" },
                new AADObject { Id = "aaduser=12345678-1234-1234-1234-123456789012;contoso.com", Name = "User 4 (ObjectID with tenant name)" },
                new AADObject { Id = "aadgroup=group1@contoso.com", Name = "Group 1 (email implicit)" },
                new AADObject { Id = "aadgroup=My Group Name;72f988bf-86f1-41af-91ab-2d7cd011db47", Name = "Group 2 (display name with tenant ID)" },
                new AADObject { Id = "aadgroup=87654321-4321-4321-4321-210987654321;contoso.com", Name = "Group 3 (ObjectID with tenant name)" },
                new AADObject { Id = "aadapp=MyApplication;72f988bf-86f1-41af-91ab-2d7cd011db47", Name = "App 1 (display name with tenant ID)" },
                new AADObject { Id = "aadapp=12345678-1234-1234-1234-123456789012;contoso.com", Name = "App 2 (ApplicationID with tenant name)" }
            };

            // Act
            var results = await _validator.ValidateAADObjectsAsync(aadObjects);

            // Assert
            Assert.Equal(9, results.Count());
            Assert.All(results, result => Assert.True(result.IsValid));
            
            var resultsList = results.ToList();
            Assert.Equal(AADObjectType.User, resultsList[0].Type);
            Assert.Equal(AADObjectType.User, resultsList[1].Type);
            Assert.Equal(AADObjectType.User, resultsList[2].Type);
            Assert.Equal(AADObjectType.User, resultsList[3].Type);
            Assert.Equal(AADObjectType.Group, resultsList[4].Type);
            Assert.Equal(AADObjectType.Group, resultsList[5].Type);
            Assert.Equal(AADObjectType.Group, resultsList[6].Type);
            Assert.Equal(AADObjectType.Application, resultsList[7].Type);
            Assert.Equal(AADObjectType.Application, resultsList[8].Type);
        }

        [Theory]
        [InlineData("AADUSER=USER@CONTOSO.COM", AADObjectType.User)]
        [InlineData("AADGROUP=GROUP@CONTOSO.COM", AADObjectType.Group)]
        [InlineData("AADAPP=MYAPP;CONTOSO.COM", AADObjectType.Application)]  // Apps must have tenant
        public async Task ValidateAADObjectAsync_WithUppercaseFormats_HandlesCorrectly(string aadId, AADObjectType expectedType)
        {
            // Arrange
            var aadObject = new AADObject { Id = aadId, Name = "Test Object" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.True(result.IsValid);
            Assert.Equal(aadId, result.Id);
            Assert.Equal(expectedType, result.Type);
        }

        [Theory]
        [InlineData("not-an-aad-id")]
        [InlineData("invalid=format")]
        [InlineData("user@contoso.com")]
        [InlineData("12345678-1234-1234-1234-123456789012")]
        [InlineData("")]
        public async Task ValidateAADObjectAsync_WithUnknownFormats_ReturnsInvalidAndUnknownType(string aadId)
        {
            // Arrange
            var aadObject = new AADObject { Id = aadId, Name = "Test Object" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.False(result.IsValid); // Enhanced mock validator validates format
            Assert.Equal(AADObjectType.Unknown, result.Type);
        }

        [Fact]
        public async Task ValidateAADObjectAsync_WithNullId_ReturnsInvalidAndUnknownType()
        {
            // Arrange
            var aadObject = new AADObject { Id = null!, Name = "Test Object" };

            // Act
            var result = await _validator.ValidateAADObjectAsync(aadObject);

            // Assert
            Assert.False(result.IsValid); // Enhanced mock validator validates format
            Assert.Equal(AADObjectType.Unknown, result.Type);
        }

        #endregion
    }
}
