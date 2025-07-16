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
            var testFilePath = Path.Join("DemoData", "ClusterScopedChanges", "multipleClusters.yml");
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

        [Fact]
        public async Task LoadAsync_ClusterMissingName_ThrowsInvalidOperationException()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                var yamlContent = @"
connections:
- url: test.eastus
  workloadGroups: []
";
                await File.WriteAllTextAsync(tempFilePath, yamlContent);
                var handler = new YamlClusterHandler(tempFilePath);

                // Act & Assert
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => handler.LoadAsync());
                Assert.Contains("Cluster at index 0 is missing a required 'name' property", exception.Message);
            }
            finally
            {
                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);
            }
        }

        [Fact]
        public async Task LoadAsync_ClusterMissingUrl_ThrowsInvalidOperationException()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                var yamlContent = @"
connections:
- name: testcluster
  workloadGroups: []
";
                await File.WriteAllTextAsync(tempFilePath, yamlContent);
                var handler = new YamlClusterHandler(tempFilePath);

                // Act & Assert
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => handler.LoadAsync());
                Assert.Contains("Cluster 'testcluster' is missing a required 'url' property", exception.Message);
            }
            finally
            {
                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);
            }
        }

        [Fact]
        public async Task LoadAsync_WorkloadGroupMissingName_ThrowsInvalidOperationException()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                var yamlContent = @"
connections:
- name: testcluster
  url: testcluster.eastus
  workloadGroups:
  - workloadGroupPolicy:
      requestRateLimitsEnforcementPolicy:
        commandsEnforcementLevel: Cluster
  - workloadGroupName: validgroup
    workloadGroupPolicy:
      requestRateLimitPolicies: []
";
                await File.WriteAllTextAsync(tempFilePath, yamlContent);
                var handler = new YamlClusterHandler(tempFilePath);

                // Act & Assert
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => handler.LoadAsync());
                Assert.Contains("Cluster 'testcluster' has a workload group at index 0 that is missing a required 'workloadGroupName' property", exception.Message);
                Assert.Contains("All workload groups must have a non-empty name", exception.Message);
            }
            finally
            {
                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);
            }
        }

        [Fact]
        public async Task LoadAsync_WorkloadGroupEmptyName_ThrowsInvalidOperationException()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                var yamlContent = @"
connections:
- name: testcluster
  url: testcluster.eastus
  workloadGroups:
  - workloadGroupName: ''
    workloadGroupPolicy:
      requestRateLimitPolicies: []
";
                await File.WriteAllTextAsync(tempFilePath, yamlContent);
                var handler = new YamlClusterHandler(tempFilePath);

                // Act & Assert
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => handler.LoadAsync());
                Assert.Contains("Cluster 'testcluster' has a workload group at index 0 that is missing a required 'workloadGroupName' property", exception.Message);
            }
            finally
            {
                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);
            }
        }

        [Fact]
        public async Task LoadAsync_WorkloadGroupWhitespaceOnlyName_ThrowsInvalidOperationException()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                var yamlContent = @"
connections:
- name: testcluster
  url: testcluster.eastus
  workloadGroups:
  - workloadGroupName: '   '
    workloadGroupPolicy:
      requestRateLimitPolicies: []
";
                await File.WriteAllTextAsync(tempFilePath, yamlContent);
                var handler = new YamlClusterHandler(tempFilePath);

                // Act & Assert
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => handler.LoadAsync());
                Assert.Contains("Cluster 'testcluster' has a workload group at index 0 that is missing a required 'workloadGroupName' property", exception.Message);
            }
            finally
            {
                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);
            }
        }

        [Fact]
        public async Task LoadAsync_ValidWorkloadGroups_DoesNotThrow()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                var yamlContent = @"
connections:
- name: testcluster
  url: testcluster.eastus
  workloadGroups:
  - workloadGroupName: group1
    workloadGroupPolicy:
      requestRateLimitPolicies:
        - limitKind: ConcurrentRequests
          scope: WorkloadGroup
          isEnabled: true
          properties:
            maxConcurrentRequests: 100
  - workloadGroupName: group2
    workloadGroupPolicy:
      requestRateLimitsEnforcementPolicy:
        commandsEnforcementLevel: Cluster
";
                await File.WriteAllTextAsync(tempFilePath, yamlContent);
                var handler = new YamlClusterHandler(tempFilePath);

                // Act
                var result = await handler.LoadAsync();

                // Assert
                Assert.NotNull(result);
                Assert.Single(result);
                var cluster = result[0];
                Assert.Equal("testcluster", cluster.Name);
                Assert.Equal("testcluster.eastus", cluster.Url);
                Assert.Equal(2, cluster.WorkloadGroups.Count);
                Assert.Equal("group1", cluster.WorkloadGroups[0].WorkloadGroupName);
                Assert.Equal("group2", cluster.WorkloadGroups[1].WorkloadGroupName);
            }
            finally
            {
                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);
            }
        }

        [Fact]
        public async Task LoadAsync_ClusterWithoutWorkloadGroups_DoesNotThrow()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                var yamlContent = @"
connections:
- name: testcluster
  url: testcluster.eastus
";
                await File.WriteAllTextAsync(tempFilePath, yamlContent);
                var handler = new YamlClusterHandler(tempFilePath);

                // Act
                var result = await handler.LoadAsync();

                // Assert
                Assert.NotNull(result);
                Assert.Single(result);
                var cluster = result[0];
                Assert.Equal("testcluster", cluster.Name);
                Assert.Equal("testcluster.eastus", cluster.Url);
                Assert.Empty(cluster.WorkloadGroups);
            }
            finally
            {
                if (File.Exists(tempFilePath))
                    File.Delete(tempFilePath);
            }
        }
    }
}
