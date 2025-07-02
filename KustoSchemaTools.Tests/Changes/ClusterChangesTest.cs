using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Moq;
using Newtonsoft.Json;
using System.Linq;
using Xunit;

namespace KustoSchemaTools.Tests.Changes
{
    public class ClusterChangesTests
    {
        private readonly Mock<ILogger> _loggerMock;

        public ClusterChangesTests()
        {
            _loggerMock = new Mock<ILogger>();
        }

        [Fact]
        public void GenerateChanges_WithIdenticalPolicies_ShouldDetectNoChanges()
        {
            // Arrange
            var oldCluster = CreateClusterWithPolicy(0.2, 1, 2, 3);
            var newCluster = CreateClusterWithPolicy(0.2, 1, 2, 3);

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            Assert.NotNull(changeSet);
            Assert.Empty(changeSet.Changes);
        }
        [Fact]
        public void GenerateChanges_WithSingleChange_ShouldDetectChangeAndCreateScript()
        {
            // Arrange
            var oldCluster = CreateClusterWithPolicy(0.2, 1, 2, 3);
            var newCluster = CreateClusterWithPolicy(0.2, 1, 2, 5);

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            Assert.NotNull(changeSet);
            Assert.NotEmpty(changeSet.Changes);
            Assert.NotEmpty(changeSet.Scripts);

            // Asserts that there is exactly one policy change in the change set
            var policyChange = Assert.Single(changeSet.Changes) as PolicyChange<ClusterCapacityPolicy>;
            Assert.NotNull(policyChange);

            // Assert that the correct script is generated
            var expectedScript = newCluster.CapacityPolicy!.ToUpdateScript();
            var actualScriptContainer = Assert.Single(changeSet.Scripts);
            Assert.Equal(expectedScript, actualScriptContainer.Script.Text);
        }

        [Fact]
        public void GenerateChanges_WithMultipleChanges_ShouldDetectAllChanges()
        {
            // Arrange
            var oldCluster = CreateClusterWithPolicy(ingestionCapacityCoreUtilizationCoefficient: 0.75, materializedViewsCapacityClusterMaximumConcurrentOperations: 10);
            var newCluster = CreateClusterWithPolicy(ingestionCapacityCoreUtilizationCoefficient: 0.95, materializedViewsCapacityClusterMaximumConcurrentOperations: 20);

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            var policyChange = Assert.Single(changeSet.Changes) as PolicyChange<ClusterCapacityPolicy>;
            Assert.NotNull(policyChange);

            // Assert that the correct script is generated
            var expectedScript = newCluster.CapacityPolicy!.ToUpdateScript();
            var actualScriptContainer = Assert.Single(changeSet.Scripts);
            Assert.Equal(expectedScript, actualScriptContainer.Script.Text);
        }

        [Fact]
        public void GenerateChanges_WithNullNewCapacityPolicy_ShouldNotGenerateChanges()
        {
            // Arrange
            var oldCluster = CreateClusterWithPolicy(ingestionCapacityCoreUtilizationCoefficient: 0.75);
            var newCluster = new Cluster { Name = oldCluster.Name, CapacityPolicy = null };

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            Assert.NotNull(changeSet);
            Assert.Empty(changeSet.Changes);
            Assert.Empty(changeSet.Scripts);
        }

        #region Helper Methods
        private Cluster CreateClusterWithPolicy(
            double? ingestionCapacityCoreUtilizationCoefficient = null,
            int? materializedViewsCapacityClusterMaximumConcurrentOperations = null,
            int? extentsRebuildClusterMaximumConcurrentOperations = null,
            int? extentsRebuildMaximumConcurrentOperationsPerNode = null
        )
        {
            return new Cluster
            {
                CapacityPolicy = new ClusterCapacityPolicy
                {
                    MaterializedViewsCapacity = new MaterializedViewsCapacity
                    {
                        ClusterMaximumConcurrentOperations = materializedViewsCapacityClusterMaximumConcurrentOperations,
                        ExtentsRebuildCapacity = (extentsRebuildClusterMaximumConcurrentOperations != null || extentsRebuildMaximumConcurrentOperationsPerNode != null) ? new ExtentsRebuildCapacity
                        {
                            ClusterMaximumConcurrentOperations = extentsRebuildClusterMaximumConcurrentOperations,
                            MaximumConcurrentOperationsPerNode = extentsRebuildMaximumConcurrentOperationsPerNode
                        } : null
                    },
                    IngestionCapacity = new IngestionCapacity
                    {
                        CoreUtilizationCoefficient = ingestionCapacityCoreUtilizationCoefficient
                    },
                }
            };
        }
        #endregion
    }
}