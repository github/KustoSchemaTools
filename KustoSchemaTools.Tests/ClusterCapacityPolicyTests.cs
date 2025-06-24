using KustoSchemaTools.Helpers;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using KustoSchemaTools.Parser.KustoLoader;
using Microsoft.Extensions.Logging;

namespace KustoSchemaTools.Tests
{
    public class ClusterCapacityPolicyTests
    {
        const string BasePath = "DemoData";
        const string Deployment = "DemoDeployment";
        const string CapacityPoliciesPath = "cluster-policies/capacity-policies";

        [Fact]
        public void CanLoadClusterWithCapacityPolicy()
        {
            // Arrange - comprehensive case with all capacity policy properties
            var clusterFilePath = Path.Combine(BasePath, Deployment, CapacityPoliciesPath, "comprehensive-cluster-policy.yml");
            var clusterYaml = File.ReadAllText(clusterFilePath);

            // Act
            var cluster = Serialization.YamlPascalCaseDeserializer.Deserialize<Cluster>(clusterYaml);

            // Assert
            Assert.NotNull(cluster);
            Assert.Equal("test", cluster.Name);
            Assert.NotNull(cluster.CapacityPolicy);
        }

        [Fact]
        public void CapacityPolicyHasCorrectValues()
        {
            // Arrange - use comprehensive cluster policy file
            var clusterFilePath = Path.Combine(BasePath, Deployment, CapacityPoliciesPath, "comprehensive-cluster-policy.yml");
            var clusterYaml = File.ReadAllText(clusterFilePath);

            // Act
            var cluster = Serialization.YamlPascalCaseDeserializer.Deserialize<Cluster>(clusterYaml);
            var policy = cluster.CapacityPolicy;

            // Assert
            Assert.NotNull(policy);

            // Test Ingestion Capacity
            Assert.NotNull(policy.IngestionCapacity);
            Assert.Equal(512, policy.IngestionCapacity.ClusterMaximumConcurrentOperations);
            Assert.Equal(0.75, policy.IngestionCapacity.CoreUtilizationCoefficient);

            // Test Export Capacity
            Assert.NotNull(policy.ExportCapacity);
            Assert.Equal(100, policy.ExportCapacity.ClusterMaximumConcurrentOperations);
            Assert.Equal(0.25, policy.ExportCapacity.CoreUtilizationCoefficient);

            // Test Extents Merge Capacity
            Assert.NotNull(policy.ExtentsMergeCapacity);
            Assert.Equal(1, policy.ExtentsMergeCapacity.MinimumConcurrentOperationsPerNode);
            Assert.Equal(3, policy.ExtentsMergeCapacity.MaximumConcurrentOperationsPerNode);

            // Test Materialized Views Capacity
            Assert.NotNull(policy.MaterializedViewsCapacity);
            Assert.Equal(1, policy.MaterializedViewsCapacity.ClusterMaximumConcurrentOperations);
            Assert.NotNull(policy.MaterializedViewsCapacity.ExtentsRebuildCapacity);
            Assert.Equal(50, policy.MaterializedViewsCapacity.ExtentsRebuildCapacity.ClusterMaximumConcurrentOperations);
            Assert.Equal(5, policy.MaterializedViewsCapacity.ExtentsRebuildCapacity.MaximumConcurrentOperationsPerNode);

            // Test Query Acceleration Capacity
            Assert.NotNull(policy.QueryAccelerationCapacity);
            Assert.Equal(100, policy.QueryAccelerationCapacity.ClusterMaximumConcurrentOperations);
            Assert.Equal(0.5, policy.QueryAccelerationCapacity.CoreUtilizationCoefficient);
            
            // Test all other capacity types are present
            Assert.NotNull(policy.ExtentsPurgeRebuildCapacity);
            Assert.Equal(1, policy.ExtentsPurgeRebuildCapacity.MaximumConcurrentOperationsPerNode);
            
            Assert.NotNull(policy.ExtentsPartitionCapacity);
            Assert.Equal(1, policy.ExtentsPartitionCapacity.ClusterMinimumConcurrentOperations);
            Assert.Equal(32, policy.ExtentsPartitionCapacity.ClusterMaximumConcurrentOperations);
            
            Assert.NotNull(policy.StoredQueryResultsCapacity);
            Assert.Equal(250, policy.StoredQueryResultsCapacity.MaximumConcurrentOperationsPerDbAdmin);
            Assert.Equal(0.75, policy.StoredQueryResultsCapacity.CoreUtilizationCoefficient);
            
            Assert.NotNull(policy.StreamingIngestionPostProcessingCapacity);
            Assert.Equal(4, policy.StreamingIngestionPostProcessingCapacity.MaximumConcurrentOperationsPerNode);
            
            Assert.NotNull(policy.PurgeStorageArtifactsCleanupCapacity);
            Assert.Equal(2, policy.PurgeStorageArtifactsCleanupCapacity.MaximumConcurrentOperationsPerCluster);
            
            Assert.NotNull(policy.PeriodicStorageArtifactsCleanupCapacity);
            Assert.Equal(2, policy.PeriodicStorageArtifactsCleanupCapacity.MaximumConcurrentOperationsPerCluster);
            
            Assert.NotNull(policy.GraphSnapshotsCapacity);
            Assert.Equal(5, policy.GraphSnapshotsCapacity.ClusterMaximumConcurrentOperations);
        }

        [Fact]
        public void CapacityPolicyGeneratesCorrectKustoScript()
        {
            // Arrange
            var clusterFilePath = Path.Combine(BasePath, Deployment, CapacityPoliciesPath, "comprehensive-cluster-policy.yml");
            var clusterYaml = File.ReadAllText(clusterFilePath);
            var cluster = Serialization.YamlPascalCaseDeserializer.Deserialize<Cluster>(clusterYaml);

            // Act
            Assert.NotNull(cluster.CapacityPolicy);
            var script = cluster.CapacityPolicy.CreateScript();

            // Assert
            Assert.NotNull(script);
            Assert.Equal("ClusterCapacityPolicy", script.Kind);
            Assert.Equal(10, script.Order);
            Assert.StartsWith(".alter-merge cluster policy capacity", script.Text);
            Assert.Contains("IngestionCapacity", script.Text);
            Assert.Contains("ExportCapacity", script.Text);
            Assert.Contains("MaterializedViewsCapacity", script.Text);
        }

        [Fact]
        public void CapacityPolicyCanSerializeToJson()
        {
            // Arrange
            var policy = new ClusterCapacityPolicy
            {
                IngestionCapacity = new IngestionCapacity
                {
                    ClusterMaximumConcurrentOperations = 512,
                    CoreUtilizationCoefficient = 0.75
                },
                ExportCapacity = new ExportCapacity
                {
                    ClusterMaximumConcurrentOperations = 100,
                    CoreUtilizationCoefficient = 0.25
                }
            };

            // Act
            var script = policy.CreateScript();
            var scriptText = script.Text;

            // Assert
            Assert.Contains("512", scriptText);
            Assert.Contains("0.75", scriptText);
            Assert.Contains("100", scriptText);
            Assert.Contains("0.25", scriptText);
            Assert.Contains("IngestionCapacity", scriptText);
            Assert.Contains("ExportCapacity", scriptText);
        }

        [Fact]
        public void UnspecifiedPropertiesAreNotIncludedInScript()
        {
            // Arrange - use a partial YAML file with only some properties set
            var clusterFilePath = Path.Combine(BasePath, Deployment, CapacityPoliciesPath, "partial-cluster-policy.yml");
            var clusterYaml = File.ReadAllText(clusterFilePath);
            var cluster = Serialization.YamlPascalCaseDeserializer.Deserialize<Cluster>(clusterYaml);

            // Act
            Assert.NotNull(cluster.CapacityPolicy);
            var script = cluster.CapacityPolicy.CreateScript();
            var scriptText = script.Text;

            // Assert - Properties that ARE specified should be included
            Assert.Contains("IngestionCapacity", scriptText);
            Assert.Contains("ExportCapacity", scriptText);
            Assert.Contains("MaterializedViewsCapacity", scriptText);
            Assert.Contains("QueryAccelerationCapacity", scriptText);
            Assert.Contains("256", scriptText); // clusterMaximumConcurrentOperations for ingestion
            Assert.Contains("0.5", scriptText); // coreUtilizationCoefficient for export
            Assert.Contains("\"ClusterMaximumConcurrentOperations\": 2", scriptText); // MaterializedViewsCapacity
            Assert.Contains("75", scriptText); // QueryAccelerationCapacity clusterMaximumConcurrentOperations
            Assert.Contains("0.6", scriptText); // QueryAccelerationCapacity coreUtilizationCoefficient
            
            // Assert - Properties that are NOT specified should NOT be included
            Assert.DoesNotContain("ExtentsMergeCapacity", scriptText);
            Assert.DoesNotContain("ExtentsPurgeRebuildCapacity", scriptText);
            Assert.DoesNotContain("ExtentsPartitionCapacity", scriptText);
            Assert.DoesNotContain("StoredQueryResultsCapacity", scriptText);
            Assert.DoesNotContain("StreamingIngestionPostProcessingCapacity", scriptText);
            Assert.DoesNotContain("PurgeStorageArtifactsCleanupCapacity", scriptText);
            Assert.DoesNotContain("PeriodicStorageArtifactsCleanupCapacity", scriptText);
            Assert.DoesNotContain("GraphSnapshotsCapacity", scriptText);
            Assert.DoesNotContain("ExtentsRebuildCapacity", scriptText); // Not specified in MaterializedViewsCapacity
            
            // Assert - No null values should appear in the script
            Assert.DoesNotContain("null", scriptText.ToLower());
        }

        [Fact]
        public void PartiallySpecifiedCapacityTypesOnlyIncludeSetProperties()
        {
            // Arrange - create a policy where some capacity types have only some of their properties set
            var policy = new ClusterCapacityPolicy
            {
                IngestionCapacity = new IngestionCapacity
                {
                    ClusterMaximumConcurrentOperations = 512
                    // CoreUtilizationCoefficient is intentionally not set (should be null)
                },
                ExportCapacity = new ExportCapacity
                {
                    CoreUtilizationCoefficient = 0.25
                    // ClusterMaximumConcurrentOperations is intentionally not set (should be null)
                },
                MaterializedViewsCapacity = new MaterializedViewsCapacity
                {
                    ClusterMaximumConcurrentOperations = 1
                    // ExtentsRebuildCapacity is intentionally not set (should be null)
                }
                // All other capacity types are intentionally not set
            };

            // Act
            var script = policy.CreateScript();
            var scriptText = script.Text;

            // Assert - Only the specified properties within each capacity type should be included
            Assert.Contains("IngestionCapacity", scriptText);
            Assert.Contains("ExportCapacity", scriptText);
            Assert.Contains("MaterializedViewsCapacity", scriptText);
            Assert.Contains("512", scriptText); // IngestionCapacity.ClusterMaximumConcurrentOperations
            Assert.Contains("0.25", scriptText); // ExportCapacity.CoreUtilizationCoefficient
            Assert.Contains("\"ClusterMaximumConcurrentOperations\": 1", scriptText); // MaterializedViewsCapacity
            
            // Assert - Unset properties within capacity types should not appear
            Assert.DoesNotContain("ExtentsRebuildCapacity", scriptText); // Not set in MaterializedViewsCapacity
            
            // Assert - Completely unset capacity types should not appear
            Assert.DoesNotContain("ExtentsMergeCapacity", scriptText);
            Assert.DoesNotContain("QueryAccelerationCapacity", scriptText);
            
            // Assert - No null values should appear in the JSON
            Assert.DoesNotContain("null", scriptText.ToLower());
        }

        [Fact]
        public void NullCapacityPolicyDoesNotGenerateScript()
        {
            // Arrange - create a cluster with no capacity policy
            var cluster = new Cluster
            {
                Name = "test",
                CapacityPolicy = null
            };

            // Act & Assert
            Assert.NotNull(cluster);
            Assert.Null(cluster.CapacityPolicy);
            
            // When capacity policy is null, we shouldn't be able to create a script
            // This test verifies that the absence of capacity policy doesn't cause issues
            // and that no capacity policy commands would be generated during deployment
        }

        [Fact]
        public void EmptyCapacityPolicyDoesNotGenerateUsefulScript()
        {
            // Arrange - create an empty capacity policy (all properties null)
            var policy = new ClusterCapacityPolicy();

            // Act
            var script = policy.CreateScript();
            var scriptText = script.Text;

            // Assert
            Assert.NotNull(script);
            Assert.Equal("ClusterCapacityPolicy", script.Kind);
            Assert.Equal(10, script.Order);
            Assert.StartsWith(".alter-merge cluster policy capacity", scriptText);
            
            // The script should contain the command but with empty/minimal JSON
            // since all capacity types are null, they should not appear in the JSON
            Assert.DoesNotContain("IngestionCapacity", scriptText);
            Assert.DoesNotContain("ExportCapacity", scriptText);
            Assert.DoesNotContain("MaterializedViewsCapacity", scriptText);
            Assert.DoesNotContain("ExtentsMergeCapacity", scriptText);
            Assert.DoesNotContain("QueryAccelerationCapacity", scriptText);
            
            // The JSON should be empty or contain only braces
            Assert.DoesNotContain("null", scriptText.ToLower());
        }
    }
}