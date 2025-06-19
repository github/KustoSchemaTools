using FluentAssertions;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Model
{
    public class ClusterModelTests
    {
        [Fact]
        public void Cluster_Should_Initialize_With_Default_Values()
        {
            // Act
            var cluster = new Cluster();

            // Assert
            cluster.Name.Should().BeNull();
            cluster.Url.Should().BeNull();
            cluster.Scripts.Should().NotBeNull().And.BeEmpty();
        }

        [Fact]
        public void Cluster_Should_Allow_Property_Assignment()
        {
            // Arrange
            var cluster = new Cluster();
            var script = new DatabaseScript("show cluster", 10);

            // Act
            cluster.Name = "TestCluster";
            cluster.Url = "https://test.kusto.windows.net";
            cluster.Scripts.Add(script);

            // Assert
            cluster.Name.Should().Be("TestCluster");
            cluster.Url.Should().Be("https://test.kusto.windows.net");
            cluster.Scripts.Should().ContainSingle().Which.Should().Be(script);
        }
    }
}
