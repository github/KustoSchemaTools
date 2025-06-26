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
        public void GenerateChanges_WithSinglePropertyChange_ShouldDetectChangeAndCreateScript()
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

            // Asserts that there is exactly one property change detected.
            // Because a nested property changed, the top-level property containing it is marked as changed.
            var propertyChange = Assert.Single(policyChange.PropertyChanges);
            Assert.Equal("MaterializedViewsCapacity", propertyChange.PropertyName);

            // Assert that the correct script is generated
            var expectedScript = newCluster.CapacityPolicy.ToUpdateScript();
            var actualScriptContainer = Assert.Single(changeSet.Scripts);
            Assert.Equal(expectedScript, actualScriptContainer.Script.Text);
        }

        // [Fact]
        // public void GenerateChanges_WithMultiplePropertyChanges_ShouldDetectAllChanges()
        // {
        //     // Arrange
        //     var oldCluster = CreateClusterWithPolicy(totalCapacity: 1000, coreUtilization: 0.75);
        //     var newCluster = CreateClusterWithPolicy(totalCapacity: 1200, coreUtilization: 0.90);

        //     // Act
        //     var changes = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

        //     // Assert
        //     var policyChange = Assert.Single(changes.PolicyChanges);
        //     var capacityChange = policyChange.Value;
        //     Assert.Equal(2, capacityChange.PropertyChanges.Count);

        //     var capacityPropChange = capacityChange.PropertyChanges.Single(p => p.PropertyName == "TotalCapacity");
        //     var coreUtilPropChange = capacityChange.PropertyChanges.Single(p => p.PropertyName == "CoreUtilizationCoefficient");

        //     Assert.Equal("1000", capacityPropChange.OldValue);
        //     Assert.Equal("1200", capacityPropChange.NewValue);

        //     Assert.Equal("0.75", coreUtilPropChange.OldValue);
        //     Assert.Equal("0.9", coreUtilPropChange.NewValue);
        // }

        // [Fact]
        // public void GenerateChanges_WithNoNewPolicyInYaml_ShouldDetectNoChanges()
        // {
        //     // Arrange
        //     var oldCluster = CreateClusterWithPolicy(totalCapacity: 1000);
        //     var newCluster = new Cluster(); // No capacity policy defined

        //     // Act
        //     var changes = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

        //     // Assert
        //     Assert.NotNull(changes);
        //     Assert.Empty(changes.PolicyChanges);
        // }

        // [Fact]
        // public void GenerateChanges_AgainstKustoDefaultPolicy_ShouldDetectChange()
        // {
        //     // Arrange
        //     // Simulate the default policy on the cluster
        //     var oldCluster = new Cluster { CapacityPolicy = new CapacityPolicy() };
        //     // The new policy explicitly sets a value that differs from the default
        //     var newCluster = CreateClusterWithPolicy(coreUtilization: 0.95);

        //     // Act
        //     var changes = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

        //     // Assert
        //     var policyChange = Assert.Single(changes.PolicyChanges).Value;
        //     var propertyChange = Assert.Single(policyChange.PropertyChanges);

        //     Assert.Equal("CoreUtilizationCoefficient", propertyChange.PropertyName);
        //     // The default value for a double property in a new object is 0.
        //     Assert.Equal("0", propertyChange.OldValue);
        //     Assert.Equal("0.95", propertyChange.NewValue);
        // }

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
                        ExtentsRebuildCapacity = new ExtentsRebuildCapacity
                        {
                            ClusterMaximumConcurrentOperations = extentsRebuildClusterMaximumConcurrentOperations,
                            MaximumConcurrentOperationsPerNode = extentsRebuildMaximumConcurrentOperationsPerNode
                        }
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