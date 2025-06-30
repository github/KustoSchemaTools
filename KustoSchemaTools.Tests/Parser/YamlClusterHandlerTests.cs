using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using System;

namespace KustoSchemaTools.Tests.Parser
{
    public class YamlClusterHandlerTests
    {
        [Fact]
        public async Task LoadAsync_ValidYamlFile_ReturnsClustersList()
        {
            // Arrange
            var testFilePath = "/workspaces/KustoSchemaTools/KustoSchemaTools.Tests/DemoData/ClusterScopedChanges/multipleClusters.yml";
            var handler = new YamlClusterHandler(testFilePath);

            // Act
            var result = await handler.LoadAsync();

            // Assert
            Assert.NotNull(result);
            Assert.Equal(2, result.Count);

            // Verify first cluster
            var cluster1 = result[0];
            Assert.Equal("test1", cluster1.Name);
            Assert.Equal("test1.eastus", cluster1.Url);
            Assert.NotNull(cluster1.CapacityPolicy);
            Assert.NotNull(cluster1.CapacityPolicy.IngestionCapacity);
            Assert.Equal(512, cluster1.CapacityPolicy.IngestionCapacity.ClusterMaximumConcurrentOperations);

            // Verify second cluster
            var cluster2 = result[1];
            Assert.Equal("test2", cluster2.Name);
            Assert.Equal("test2.eastus", cluster2.Url);
            Assert.NotNull(cluster2.CapacityPolicy);
            Assert.NotNull(cluster2.CapacityPolicy.IngestionCapacity);
            Assert.Equal(500, cluster2.CapacityPolicy.IngestionCapacity.ClusterMaximumConcurrentOperations);
        }

        [Fact]
        public async Task LoadAsync_FileNotFound_ThrowsFileNotFoundException()
        {
            // Arrange
            var nonExistentPath = "/path/that/does/not/exist/clusters.yml";
            var handler = new YamlClusterHandler(nonExistentPath);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<FileNotFoundException>(() => handler.LoadAsync());
            Assert.Contains("Clusters file not found at path", exception.Message);
        }

        [Fact]
        public async Task LoadAsync_EmptyFile_ThrowsInvalidOperationException()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                await File.WriteAllTextAsync(tempFilePath, "");
                var handler = new YamlClusterHandler(tempFilePath);

                // Act & Assert
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => handler.LoadAsync());
                Assert.Contains("Clusters file is empty", exception.Message);
            }
            finally
            {
                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);
            }
        }

        [Fact]
        public async Task LoadAsync_InvalidYaml_ThrowsInvalidOperationException()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                await File.WriteAllTextAsync(tempFilePath, "invalid: yaml: content: [");
                var handler = new YamlClusterHandler(tempFilePath);

                // Act & Assert
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => handler.LoadAsync());
                Assert.Contains("Failed to parse clusters file", exception.Message);
            }
            finally
            {
                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);
            }
        }

        [Fact]
        public async Task LoadAsync_InvalidClustersProperties_ThrowsInvalidOperationException()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                await File.WriteAllTextAsync(tempFilePath, "someOtherProperty: value");
                var handler = new YamlClusterHandler(tempFilePath);

                // Act & Assert
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => handler.LoadAsync());
                Assert.Contains("Failed to parse clusters file", exception.Message);
                Assert.Contains("Property 'someOtherProperty' not found on type 'KustoSchemaTools.Model.Clusters'.", exception.Message);
            }
            finally
            {
                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);
            }
        }
    }
}
