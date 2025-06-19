using FluentAssertions;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Model
{
    public class AADObjectModelTests
    {
        [Fact]
        public void AADObject_Should_Initialize_With_Default_Values()
        {
            // Act
            var aadObject = new AADObject();

            // Assert
            aadObject.Name.Should().BeNull();
            aadObject.Id.Should().BeNull();
        }

        [Fact]
        public void AADObject_Should_Allow_Property_Assignment()
        {
            // Arrange
            var aadObject = new AADObject();

            // Act
            aadObject.Name = "test@example.com";
            aadObject.Id = "12345678-1234-1234-1234-123456789012";

            // Assert
            aadObject.Name.Should().Be("test@example.com");
            aadObject.Id.Should().Be("12345678-1234-1234-1234-123456789012");
        }

        [Fact]
        public void AADObject_Should_Support_Equality_Comparison()
        {
            // Arrange
            var aadObject1 = new AADObject { Name = "test@example.com", Id = "12345" };
            var aadObject2 = new AADObject { Name = "test@example.com", Id = "12345" };
            var aadObject3 = new AADObject { Name = "different@example.com", Id = "12345" };

            // Act & Assert
            aadObject1.Should().BeEquivalentTo(aadObject2);
            aadObject1.Should().NotBeEquivalentTo(aadObject3);
        }
    }
}
