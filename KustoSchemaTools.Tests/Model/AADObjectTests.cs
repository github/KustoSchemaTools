using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Model
{
    public class AADObjectTests
    {
        [Fact]
        public void AADObject_DefaultConstructor_InitializesCorrectly()
        {
            // Act
            var aadObject = new AADObject();

            // Assert
            Assert.Null(aadObject.Id);
            Assert.Null(aadObject.Name);
        }

        [Fact]
        public void AADObject_WithProperties_SetsCorrectly()
        {
            // Arrange
            var id = "aaduser=test@contoso.com";
            var name = "Test User";

            // Act
            var aadObject = new AADObject
            {
                Id = id,
                Name = name
            };

            // Assert
            Assert.Equal(id, aadObject.Id);
            Assert.Equal(name, aadObject.Name);
        }

        [Theory]
        [InlineData("aaduser=user@domain.com")]
        [InlineData("aadgroup=group@domain.com")]
        [InlineData("aadapp=12345678-1234-1234-1234-123456789012")]
        public void AADObject_WithValidIds_AcceptsAllFormats(string validId)
        {
            // Act
            var aadObject = new AADObject { Id = validId };

            // Assert
            Assert.Equal(validId, aadObject.Id);
        }

        [Fact]
        public void AADObject_Equality_WorksCorrectly()
        {
            // Arrange
            var aadObject1 = new AADObject { Id = "aaduser=test@contoso.com", Name = "Test User" };
            var aadObject2 = new AADObject { Id = "aaduser=test@contoso.com", Name = "Test User" };
            var aadObject3 = new AADObject { Id = "aaduser=different@contoso.com", Name = "Test User" };

            // Act & Assert
            Assert.Equal(aadObject1.Id, aadObject2.Id);
            Assert.NotEqual(aadObject1.Id, aadObject3.Id);
        }

        [Fact]
        public void AADObject_WithEmptyValues_HandlesCorrectly()
        {
            // Act
            var aadObject = new AADObject
            {
                Id = "",
                Name = ""
            };

            // Assert
            Assert.Equal("", aadObject.Id);
            Assert.Equal("", aadObject.Name);
        }
    }
}
