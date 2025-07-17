using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Moq;

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
            var oldCluster = CreateClusterWithPolicy(0.2, 1, 2);
            var newCluster = CreateClusterWithPolicy(0.2, 1, 2);

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
            var oldCluster = CreateClusterWithPolicy(0.2, 1, 2);
            var newCluster = CreateClusterWithPolicy(0.2, 1, 5);

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

        #region Workload Group Tests

        [Fact]
        public void GenerateChanges_WithNewWorkloadGroup_ShouldDetectCreation()
        {
            // Arrange
            var oldCluster = new Cluster { Name = "TestCluster", WorkloadGroups = new List<WorkloadGroup>() };
            var newCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> 
                {
                    CreateWorkloadGroup("test-group", maxMemoryPerQueryPerNode: 1024)
                }
            };

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            Assert.NotNull(changeSet);
            Assert.NotEmpty(changeSet.Changes);
            Assert.NotEmpty(changeSet.Scripts);

            var policyChange = Assert.Single(changeSet.Changes) as PolicyChange<WorkloadGroupPolicy>;
            Assert.NotNull(policyChange);
            Assert.Equal("test-group", policyChange.Entity);

            var scriptContainer = Assert.Single(changeSet.Scripts);
            Assert.Contains(".create-or-alter workload_group test-group", scriptContainer.Script.Text);
        }

        [Fact]
        public void GenerateChanges_WithUpdatedWorkloadGroup_ShouldDetectUpdate()
        {
            // Arrange
            var oldCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> 
                {
                    CreateWorkloadGroup("test-group", maxMemoryPerQueryPerNode: 1024)
                }
            };
            var newCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> 
                {
                    CreateWorkloadGroup("test-group", maxMemoryPerQueryPerNode: 2048)
                }
            };

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            Assert.NotNull(changeSet);
            Assert.NotEmpty(changeSet.Changes);
            Assert.NotEmpty(changeSet.Scripts);

            var policyChange = Assert.Single(changeSet.Changes) as PolicyChange<WorkloadGroupPolicy>;
            Assert.NotNull(policyChange);
            Assert.Equal("test-group", policyChange.Entity);

            var scriptContainer = Assert.Single(changeSet.Scripts);
            Assert.Contains(".alter-merge workload_group test-group", scriptContainer.Script.Text);
        }

        [Fact]
        public void GenerateChanges_WithIdenticalWorkloadGroups_ShouldDetectNoChanges()
        {
            // Arrange
            var workloadGroup = CreateWorkloadGroup("test-group", maxMemoryPerQueryPerNode: 1024);
            var oldCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> { workloadGroup }
            };
            var newCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> 
                {
                    CreateWorkloadGroup("test-group", maxMemoryPerQueryPerNode: 1024)
                }
            };

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            Assert.NotNull(changeSet);
            Assert.Empty(changeSet.Changes);
            Assert.Empty(changeSet.Scripts);
        }

        [Fact]
        public void GenerateChanges_WithWorkloadGroupDeletion_ShouldDetectDeletion()
        {
            // Arrange
            var oldCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> 
                {
                    CreateWorkloadGroup("test-group", maxMemoryPerQueryPerNode: 1024)
                }
            };
            var newCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup>(),
                Deletions = new ClusterDeletions 
                {
                    WorkloadGroups = new List<string> { "test-group" }
                }
            };

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            Assert.NotNull(changeSet);
            Assert.NotEmpty(changeSet.Changes);

            var deletionChange = Assert.Single(changeSet.Changes) as DeletionChange;
            Assert.NotNull(deletionChange);
            Assert.Equal("test-group", deletionChange.Entity);
            Assert.Equal("workload_group", deletionChange.EntityType);
            Assert.Contains("Drop Workload Group test-group", deletionChange.Markdown);
        }

        [Fact]
        public void GenerateChanges_WithWorkloadGroupDeletionOfNonExistentGroup_ShouldNotCreateChange()
        {
            // Arrange
            var oldCluster = new Cluster { Name = "TestCluster", WorkloadGroups = new List<WorkloadGroup>() };
            var newCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup>(),
                Deletions = new ClusterDeletions 
                {
                    WorkloadGroups = new List<string> { "non-existent-group" }
                }
            };

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            Assert.NotNull(changeSet);
            Assert.Empty(changeSet.Changes);
            Assert.Empty(changeSet.Scripts);
        }

        [Fact]
        public void GenerateChanges_WithWorkloadGroupMarkedForDeletionButAlsoInNewList_ShouldOnlyProcessDeletion()
        {
            // Arrange
            var oldCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> 
                {
                    CreateWorkloadGroup("test-group", maxMemoryPerQueryPerNode: 1024)
                }
            };
            var newCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> 
                {
                    CreateWorkloadGroup("test-group", maxMemoryPerQueryPerNode: 2048)
                },
                Deletions = new ClusterDeletions 
                {
                    WorkloadGroups = new List<string> { "test-group" }
                }
            };

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            Assert.NotNull(changeSet);
            Assert.NotEmpty(changeSet.Changes);

            var deletionChange = Assert.Single(changeSet.Changes) as DeletionChange;
            Assert.NotNull(deletionChange);
            Assert.Equal("test-group", deletionChange.Entity);
            Assert.Equal("workload_group", deletionChange.EntityType);
        }

        [Fact]
        public void GenerateChanges_WithMultipleWorkloadGroupChanges_ShouldDetectAllChanges()
        {
            // Arrange
            var oldCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> 
                {
                    CreateWorkloadGroup("group1", maxMemoryPerQueryPerNode: 1024),
                    CreateWorkloadGroup("group2", maxMemoryPerQueryPerNode: 2048),
                    CreateWorkloadGroup("group3", maxMemoryPerQueryPerNode: 512)
                }
            };
            var newCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> 
                {
                    CreateWorkloadGroup("group1", maxMemoryPerQueryPerNode: 1024), // No change
                    CreateWorkloadGroup("group2", maxMemoryPerQueryPerNode: 4096), // Update
                    CreateWorkloadGroup("group4", maxMemoryPerQueryPerNode: 256)   // New
                },
                Deletions = new ClusterDeletions 
                {
                    WorkloadGroups = new List<string> { "group3" } // Delete
                }
            };

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            Assert.NotNull(changeSet);
            Assert.Equal(3, changeSet.Changes.Count); // 1 deletion + 1 update + 1 creation

            // Check deletion
            var deletionChange = changeSet.Changes.OfType<DeletionChange>().Single();
            Assert.Equal("group3", deletionChange.Entity);

            // Check updates/creations
            var policyChanges = changeSet.Changes.OfType<PolicyChange<WorkloadGroupPolicy>>().ToList();
            Assert.Equal(2, policyChanges.Count);
            
            var group2Change = policyChanges.First(c => c.Entity == "group2");
            var group4Change = policyChanges.First(c => c.Entity == "group4");
            
            Assert.NotNull(group2Change);
            Assert.NotNull(group4Change);

            // Verify scripts
            Assert.Equal(3, changeSet.Scripts.Count);
        }

        [Fact]
        public void GenerateChanges_WithWorkloadGroupHavingComplexPolicy_ShouldDetectChanges()
        {
            // Arrange
            var oldCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> 
                {
                    CreateComplexWorkloadGroup("complex-group", 1024, TimeSpan.FromMinutes(5))
                }
            };
            var newCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> 
                {
                    CreateComplexWorkloadGroup("complex-group", 2048, TimeSpan.FromMinutes(10))
                }
            };

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            Assert.NotNull(changeSet);
            Assert.NotEmpty(changeSet.Changes);

            var policyChange = Assert.Single(changeSet.Changes) as PolicyChange<WorkloadGroupPolicy>;
            Assert.NotNull(policyChange);
            Assert.Equal("complex-group", policyChange.Entity);
            Assert.Contains("MaxMemoryPerQueryPerNode", policyChange.Markdown);
            Assert.Contains("MaxExecutionTime", policyChange.Markdown);
        }

        [Fact]
        public void GenerateChanges_WithWorkloadGroupHavingNullPolicy_ShouldNotCreateChange()
        {
            // Arrange
            var oldCluster = new Cluster { Name = "TestCluster", WorkloadGroups = new List<WorkloadGroup>() };
            var newCluster = new Cluster 
            { 
                Name = "TestCluster", 
                WorkloadGroups = new List<WorkloadGroup> 
                {
                    new WorkloadGroup { WorkloadGroupName = "null-policy-group", WorkloadGroupPolicy = null }
                }
            };

            // Act
            var changeSet = ClusterChanges.GenerateChanges(oldCluster, newCluster, _loggerMock.Object);

            // Assert
            Assert.NotNull(changeSet);
            Assert.Empty(changeSet.Changes);
            Assert.Empty(changeSet.Scripts);
        }

        #endregion

        #region Helper Methods
        private Cluster CreateClusterWithPolicy(
            double? ingestionCapacityCoreUtilizationCoefficient = null,
            int? materializedViewsCapacityClusterMaximumConcurrentOperations = null,
            int? materializedViewsCapacityClusterMinimumConcurrentOperations = null
        )
        {
            return new Cluster
            {
                CapacityPolicy = new ClusterCapacityPolicy
                {
                    MaterializedViewsCapacity = new MaterializedViewsCapacity
                    {
                        ClusterMaximumConcurrentOperations = materializedViewsCapacityClusterMaximumConcurrentOperations,
                        ClusterMinimumConcurrentOperations = materializedViewsCapacityClusterMinimumConcurrentOperations
                    },
                    IngestionCapacity = new IngestionCapacity
                    {
                        CoreUtilizationCoefficient = ingestionCapacityCoreUtilizationCoefficient
                    },
                }
            };
        }

        private WorkloadGroup CreateWorkloadGroup(string name, long? maxMemoryPerQueryPerNode = null)
        {
            return new WorkloadGroup
            {
                WorkloadGroupName = name,
                WorkloadGroupPolicy = new WorkloadGroupPolicy
                {
                    RequestLimitsPolicy = new RequestLimitsPolicy
                    {
                        MaxMemoryPerQueryPerNode = maxMemoryPerQueryPerNode.HasValue 
                            ? new PolicyValue<long> { Value = maxMemoryPerQueryPerNode.Value, IsRelaxable = false }
                            : null
                    }
                }
            };
        }

        private WorkloadGroup CreateComplexWorkloadGroup(string name, long maxMemoryPerQueryPerNode, TimeSpan maxExecutionTime)
        {
            return new WorkloadGroup
            {
                WorkloadGroupName = name,
                WorkloadGroupPolicy = new WorkloadGroupPolicy
                {
                    RequestLimitsPolicy = new RequestLimitsPolicy
                    {
                        MaxMemoryPerQueryPerNode = new PolicyValue<long> { Value = maxMemoryPerQueryPerNode, IsRelaxable = false },
                        MaxExecutionTime = new PolicyValue<TimeSpan> { Value = maxExecutionTime, IsRelaxable = true },
                        MaxResultRecords = new PolicyValue<long> { Value = 10000, IsRelaxable = false }
                    },
                    RequestRateLimitPolicies = new List<RequestRateLimitPolicy>
                    {
                        new RequestRateLimitPolicy
                        {
                            IsEnabled = true,
                            Scope = RateLimitScope.WorkloadGroup,
                            LimitKind = RateLimitKind.ConcurrentRequests,
                            Properties = new RateLimitProperties
                            {
                                MaxConcurrentRequests = 100
                            }
                        },
                        new RequestRateLimitPolicy
                        {
                            IsEnabled = true,
                            Scope = RateLimitScope.Principal,
                            LimitKind = RateLimitKind.ResourceUtilization,
                            Properties = new RateLimitProperties
                            {
                                ResourceKind = RateLimitResourceKind.TotalCpuSeconds,
                                MaxUtilization = 0.8,
                                TimeWindow = TimeSpan.FromMinutes(5)
                            }
                        }
                    }
                }
            };
        }
        #endregion
    }
}