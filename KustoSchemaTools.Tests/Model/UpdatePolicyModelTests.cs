using FluentAssertions;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Model
{
    public class UpdatePolicyModelTests
    {
        [Fact]
        public void UpdatePolicy_Should_Initialize_With_Default_Values()
        {
            // Act
            var updatePolicy = new UpdatePolicy();

            // Assert
            updatePolicy.Source.Should().BeNull();
            updatePolicy.Query.Should().BeNull();
            updatePolicy.ManagedIdentity.Should().BeNull();
            updatePolicy.IsEnabled.Should().BeTrue();
            updatePolicy.IsTransactional.Should().BeFalse();
            updatePolicy.PropagateIngestionProperties.Should().BeTrue(); // Default is true
        }

        [Fact]
        public void UpdatePolicy_Should_Allow_Property_Assignment()
        {
            // Arrange
            var updatePolicy = new UpdatePolicy();

            // Act
            updatePolicy.Source = "SourceTable";
            updatePolicy.Query = "SourceTable | extend ProcessedTime = now()";
            updatePolicy.ManagedIdentity = "system";
            updatePolicy.IsEnabled = false;
            updatePolicy.IsTransactional = true;
            updatePolicy.PropagateIngestionProperties = true;

            // Assert
            updatePolicy.Source.Should().Be("SourceTable");
            updatePolicy.Query.Should().Be("SourceTable | extend ProcessedTime = now()");
            updatePolicy.ManagedIdentity.Should().Be("system");
            updatePolicy.IsEnabled.Should().BeFalse();
            updatePolicy.IsTransactional.Should().BeTrue();
            updatePolicy.PropagateIngestionProperties.Should().BeTrue();
        }
    }
}
