using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using Microsoft.Extensions.Logging;
using Moq;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;
using System.Data;
using System;
using System.Linq;
using Kusto.Data.Common;

namespace KustoSchemaTools.Tests
{
    public class KustoClusterOrchestratorTests
    {
        private readonly Mock<ILogger<KustoClusterOrchestrator>> loggerMock;
        private readonly Mock<IKustoClusterHandlerFactory> kustoClusterHandlerFactoryMock;
        private readonly Mock<KustoClusterHandler> kustoHandlerMock;
        private readonly KustoClusterOrchestrator orchestrator;

        public KustoClusterOrchestratorTests()
        {
            loggerMock = new Mock<ILogger<KustoClusterOrchestrator>>();
            kustoClusterHandlerFactoryMock = new Mock<IKustoClusterHandlerFactory>();
            
            // Create mock for KustoClusterHandler
            var kustoClientMock = new Mock<KustoClient>("test.eastus");
            var kustoLoggerMock = new Mock<ILogger<KustoClusterHandler>>();
            kustoHandlerMock = new Mock<KustoClusterHandler>(kustoClientMock.Object, kustoLoggerMock.Object, "test", "test.eastus");
            
            orchestrator = new KustoClusterOrchestrator(
                loggerMock.Object,
                kustoClusterHandlerFactoryMock.Object);
        }

        private Clusters CreateClustersWithCapacityPolicy(ClusterCapacityPolicy? capacityPolicy = null)
        {
            return new Clusters
            {
                Connections = new List<Cluster> 
                { 
                    new Cluster 
                    { 
                        Name = "test", 
                        Url = "test.eastus",
                        CapacityPolicy = capacityPolicy
                    } 
                }
            };
        }

        private void SetupMockHandler(Cluster kustoCluster)
        {
            // Configure the handler factory to return our mock handler
            kustoClusterHandlerFactoryMock
                .Setup(f => f.Create("test", "test.eastus"))
                .Returns(kustoHandlerMock.Object);
                
            // Set up the mock handler to return our test cluster
            kustoHandlerMock
                .Setup(h => h.LoadAsync())
                .ReturnsAsync(kustoCluster);
        }

        private Clusters CreateMultipleClusters()
        {
            return new Clusters
            {
                Connections = new List<Cluster>
                {
                    new Cluster
                    {
                        Name = "cluster1",
                        Url = "cluster1.eastus",
                        CapacityPolicy = new ClusterCapacityPolicy
                        {
                            IngestionCapacity = new IngestionCapacity
                            {
                                ClusterMaximumConcurrentOperations = 500,
                                CoreUtilizationCoefficient = 0.75
                            }
                        }
                    },
                    new Cluster
                    {
                        Name = "cluster2",
                        Url = "cluster2.westus",
                        CapacityPolicy = new ClusterCapacityPolicy
                        {
                            IngestionCapacity = new IngestionCapacity
                            {
                                ClusterMaximumConcurrentOperations = 600,
                                CoreUtilizationCoefficient = 0.85
                            }
                        }
                    }
                }
            };
        }

        private void SetupMultipleClusterMocks()
        {
            // Mock for cluster1
            var kustoHandler1Mock = new Mock<KustoClusterHandler>(new Mock<KustoClient>("cluster1.eastus").Object, new Mock<ILogger<KustoClusterHandler>>().Object, "cluster1", "cluster1.eastus");
            var kustoCluster1 = new Cluster
            {
                Name = "cluster1",
                CapacityPolicy = new ClusterCapacityPolicy
                {
                    IngestionCapacity = new IngestionCapacity
                    {
                        ClusterMaximumConcurrentOperations = 300, // Different from config
                        CoreUtilizationCoefficient = 0.5
                    }
                }
            };
            
            kustoClusterHandlerFactoryMock
                .Setup(f => f.Create("cluster1", "cluster1.eastus"))
                .Returns(kustoHandler1Mock.Object);
            kustoHandler1Mock
                .Setup(h => h.LoadAsync())
                .ReturnsAsync(kustoCluster1);

            // Mock for cluster2 - same as config, no changes
            var kustoHandler2Mock = new Mock<KustoClusterHandler>(new Mock<KustoClient>("cluster2.westus").Object, new Mock<ILogger<KustoClusterHandler>>().Object, "cluster2", "cluster2.westus");
            var kustoCluster2 = new Cluster
            {
                Name = "cluster2",
                CapacityPolicy = new ClusterCapacityPolicy
                {
                    IngestionCapacity = new IngestionCapacity
                    {
                        ClusterMaximumConcurrentOperations = 600, // Same as config
                        CoreUtilizationCoefficient = 0.85
                    }
                }
            };
            
            kustoClusterHandlerFactoryMock
                .Setup(f => f.Create("cluster2", "cluster2.westus"))
                .Returns(kustoHandler2Mock.Object);
            kustoHandler2Mock
                .Setup(h => h.LoadAsync())
                .ReturnsAsync(kustoCluster2);
        }

        [Fact]
        public async Task GenerateChangesAsync_EmptyCapacityPolicy_ReturnsEmptyChangeSet()
        {
            // Arrange
            var clusters = CreateClustersWithCapacityPolicy();
            var kustoCluster = new Cluster { Name = "test" };
            SetupMockHandler(kustoCluster);

            // Act
            var changes = await orchestrator.GenerateChangesAsync(clusters);

            // Assert
            Assert.Single(changes);
            var changeSet = changes[0];
            Assert.Empty(changeSet.Changes);
        }

        [Fact]
        public async Task GenerateChangesAsync_WithCapacityPolicyChanges_ReturnsChangeset()
        {
            // Arrange
            var newCapacityPolicy = new ClusterCapacityPolicy
            {
                IngestionCapacity = new IngestionCapacity
                {
                    ClusterMaximumConcurrentOperations = 500,
                    CoreUtilizationCoefficient = 0.75
                }
            };
            
            var clusters = CreateClustersWithCapacityPolicy(newCapacityPolicy);

            // Create a cluster with different capacity policy settings to trigger changes
            var kustoCluster = new Cluster 
            { 
                Name = "test",
                CapacityPolicy = new ClusterCapacityPolicy
                {
                    IngestionCapacity = new IngestionCapacity
                    {
                        ClusterMaximumConcurrentOperations = 300,
                    }
                }
            };
            
            SetupMockHandler(kustoCluster);

            // Act
            var changes = await orchestrator.GenerateChangesAsync(clusters);

            // Assert
            Assert.Single(changes);
            var changeSet = changes[0];
            Assert.NotEmpty(changeSet.Changes);
            Assert.Single(changeSet.Changes);
            var policyChange = Assert.IsType<PolicyChange<ClusterCapacityPolicy>>(changeSet.Changes[0]);
            // Assert.Single(policyChange.PropertyChanges);
            // var propertyChange = Assert.Single(policyChange.PropertyChanges);
            // Assert.Equal("IngestionCapacity", propertyChange.PropertyName);
            // Assert.NotEqual(propertyChange.OldValue, propertyChange.NewValue);
            Assert.NotEmpty(policyChange.Scripts);
        }

        [Fact]
        public async Task GenerateChangesAsync_SameCapacityPolicy_ReturnsEmptyChangeSet()
        {
            // Arrange
            var capacityPolicy = new ClusterCapacityPolicy
            {
                IngestionCapacity = new IngestionCapacity
                {
                    ClusterMaximumConcurrentOperations = 400,
                    CoreUtilizationCoefficient = 0.8
                }
            };
            
            var clusters = CreateClustersWithCapacityPolicy(capacityPolicy);

            // Create a cluster with the same capacity policy settings - no changes should be detected
            var kustoCluster = new Cluster 
            { 
                Name = "test",
                CapacityPolicy = new ClusterCapacityPolicy
                {
                    IngestionCapacity = new IngestionCapacity
                    {
                        ClusterMaximumConcurrentOperations = 400,
                        CoreUtilizationCoefficient = 0.8
                    }
                }
            };
            
            SetupMockHandler(kustoCluster);

            // Act
            var changes = await orchestrator.GenerateChangesAsync(clusters);

            // Assert
            Assert.Single(changes);
            var changeSet = changes[0];
            Assert.Empty(changeSet.Changes);
        }

        [Fact]
        public async Task GenerateChangesAsync_MultipleClusters_ReturnsCorrectChangesets()
        {
            // Arrange
            var clusters = CreateMultipleClusters();
            SetupMultipleClusterMocks();

            // Act
            var changes = await orchestrator.GenerateChangesAsync(clusters);

            // Assert
            Assert.Equal(2, changes.Count);
            
            // First cluster should have changes
            var cluster1ChangeSet = changes.FirstOrDefault(c => c.Entity == "cluster1");
            Assert.NotNull(cluster1ChangeSet);
            Assert.NotEmpty(cluster1ChangeSet.Changes);
            Assert.Single(cluster1ChangeSet.Changes);
            
            var cluster1PolicyChange = Assert.IsType<PolicyChange<ClusterCapacityPolicy>>(cluster1ChangeSet.Changes[0]);
            // Assert.Single(cluster1PolicyChange.PropertyChanges);
            // Assert.Equal("IngestionCapacity", cluster1PolicyChange.PropertyChanges[0].PropertyName);
            
            // Second cluster should have no changes
            var cluster2ChangeSet = changes.FirstOrDefault(c => c.Entity == "cluster2");
            Assert.NotNull(cluster2ChangeSet);
            Assert.Empty(cluster2ChangeSet.Changes);
        }

        [Fact]
        public async Task GenerateChangesAsync_NullClusters_ThrowsNullReferenceException()
        {
            // Act & Assert
            await Assert.ThrowsAsync<NullReferenceException>(() => orchestrator.GenerateChangesAsync(null!));
        }

        [Fact]
        public async Task GenerateChangesAsync_EmptyClustersList_ReturnsEmptyList()
        {
            // Arrange
            var clusters = new Clusters { Connections = new List<Cluster>() };

            // Act
            var changes = await orchestrator.GenerateChangesAsync(clusters);

            // Assert
            Assert.Empty(changes);
        }

        [Fact]
        public async Task GenerateChangesAsync_LoadAsyncThrowsException_PropagatesException()
        {
            // Arrange
            var clusters = CreateClustersWithCapacityPolicy();
            
            kustoClusterHandlerFactoryMock
                .Setup(f => f.Create("test", "test.eastus"))
                .Returns(kustoHandlerMock.Object);
            
            kustoHandlerMock
                .Setup(h => h.LoadAsync())
                .ThrowsAsync(new Exception("Connection failed"));

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => orchestrator.GenerateChangesAsync(clusters));
            Assert.Equal("Connection failed", exception.Message);
        }

        [Fact]
        public async Task GenerateChangesAsync_MismatchedClusterNames_ThrowsArgumentException()
        {
            // Arrange
            var clusters = CreateClustersWithCapacityPolicy();
            var kustoCluster = new Cluster { Name = "different-name" }; // Different name
            SetupMockHandler(kustoCluster);

            // Act & Assert
            var exception = await Assert.ThrowsAsync<ArgumentException>(() => orchestrator.GenerateChangesAsync(clusters));
            Assert.Contains("Cluster names must match", exception.Message);
        }

        [Fact]
        public async Task GenerateChangesAsync_NewClusterHasNullPolicy_ReturnsEmptyChanges()
        {
            // Arrange - clusters with null capacity policy
            var clusters = CreateClustersWithCapacityPolicy(null);
            var kustoCluster = new Cluster 
            { 
                Name = "test",
                CapacityPolicy = new ClusterCapacityPolicy
                {
                    IngestionCapacity = new IngestionCapacity
                    {
                        ClusterMaximumConcurrentOperations = 300
                    }
                }
            };
            SetupMockHandler(kustoCluster);

            // Act
            var changes = await orchestrator.GenerateChangesAsync(clusters);

            // Assert
            Assert.Single(changes);
            var changeSet = changes[0];
            Assert.Empty(changeSet.Changes); // No changes because new policy is null
        }

        [Fact]
        public async Task GenerateChangesAsync_VerifyFactoryCalledWithCorrectUrl()
        {
            // Arrange
            var clusters = CreateClustersWithCapacityPolicy();
            var kustoCluster = new Cluster { Name = "test" };
            SetupMockHandler(kustoCluster);

            // Act
            await orchestrator.GenerateChangesAsync(clusters);

            // Assert
            kustoClusterHandlerFactoryMock.Verify(f => f.Create("test", "test.eastus"), Times.Once);
            kustoHandlerMock.Verify(h => h.LoadAsync(), Times.Once);
        }

        [Fact]
        public async Task GenerateChangesAsync_VerifyLoggingCalled()
        {
            // Arrange
            var clusters = CreateClustersWithCapacityPolicy();
            var kustoCluster = new Cluster { Name = "test" };
            SetupMockHandler(kustoCluster);

            // Act
            await orchestrator.GenerateChangesAsync(clusters);

            // Assert - Verify that logging was called with the expected message
            loggerMock.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Generating cluster diff for test")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task GenerateChangesFromFileAsync_ValidYamlFile_ReturnsChanges()
        {
            // Arrange
            var yamlFilePath = Path.Combine("DemoData", "ClusterScopedChanges", "multipleClusters.yml");
            
            // Set up mocks for the clusters defined in the YAML file
            var kustoHandler1Mock = new Mock<KustoClusterHandler>(new Mock<KustoClient>("test1.eastus").Object, new Mock<ILogger<KustoClusterHandler>>().Object, "test1", "test1.eastus");
            var kustoCluster1 = new Cluster
            {
                Name = "test1",
                CapacityPolicy = new ClusterCapacityPolicy
                {
                    IngestionCapacity = new IngestionCapacity
                    {
                        ClusterMaximumConcurrentOperations = 300, // Different from YAML
                        CoreUtilizationCoefficient = 0.5
                    }
                }
            };
            
            kustoClusterHandlerFactoryMock
                .Setup(f => f.Create("test1", "test1.eastus"))
                .Returns(kustoHandler1Mock.Object);
            kustoHandler1Mock
                .Setup(h => h.LoadAsync())
                .ReturnsAsync(kustoCluster1);

            var kustoHandler2Mock = new Mock<KustoClusterHandler>(new Mock<KustoClient>("test2.eastus").Object, new Mock<ILogger<KustoClusterHandler>>().Object, "test2", "test2.eastus");
            var kustoCluster2 = new Cluster
            {
                Name = "test2",
                CapacityPolicy = new ClusterCapacityPolicy
                {
                    IngestionCapacity = new IngestionCapacity
                    {
                        ClusterMaximumConcurrentOperations = 500, // Same as YAML
                        CoreUtilizationCoefficient = 0.8
                    }
                }
            };
            
            kustoClusterHandlerFactoryMock
                .Setup(f => f.Create("test2", "test2.eastus"))
                .Returns(kustoHandler2Mock.Object);
            kustoHandler2Mock
                .Setup(h => h.LoadAsync())
                .ReturnsAsync(kustoCluster2);

            // Act
            var changes = await orchestrator.GenerateChangesFromFileAsync(yamlFilePath);

            // Assert
            Assert.Equal(2, changes.Count);
            
            // test1 should have changes
            var test1ChangeSet = changes.FirstOrDefault(c => c.Entity == "test1");
            Assert.NotNull(test1ChangeSet);
            Assert.NotEmpty(test1ChangeSet.Changes);
            
            // test2 should have no changes (same configuration)
            var test2ChangeSet = changes.FirstOrDefault(c => c.Entity == "test2");
            Assert.NotNull(test2ChangeSet);
            Assert.Empty(test2ChangeSet.Changes);
        }

        [Fact]
        public async Task GenerateChangesFromFileAsync_NonExistentFile_ThrowsFileNotFoundException()
        {
            // Arrange
            var nonExistentFilePath = "non-existent-file.yml";

            // Act & Assert
            var exception = await Assert.ThrowsAsync<FileNotFoundException>(() => orchestrator.GenerateChangesFromFileAsync(nonExistentFilePath));
            Assert.Contains("Clusters file not found", exception.Message);
        }

        [Fact]
        public async Task GenerateChangesFromFileAsync_EmptyFile_ThrowsInvalidOperationException()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                // Create an empty file
                await File.WriteAllTextAsync(tempFilePath, "");

                // Act & Assert
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => orchestrator.GenerateChangesFromFileAsync(tempFilePath));
                Assert.Contains("Clusters file is empty", exception.Message);
            }
            finally
            {
                File.Delete(tempFilePath);
            }
        }

        [Fact]
        public async Task GenerateChangesFromFileAsync_InvalidYaml_ThrowsInvalidOperationException()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                // Create a file with invalid YAML
                await File.WriteAllTextAsync(tempFilePath, "invalid: yaml: content: [");

                // Act & Assert
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => orchestrator.GenerateChangesFromFileAsync(tempFilePath));
                Assert.Contains("Failed to parse clusters file", exception.Message);
            }
            finally
            {
                File.Delete(tempFilePath);
            }
        }

        [Fact]
        public async Task GenerateChangesFromFileAsync_ValidYamlWithEmptyConnections_ReturnsEmptyList()
        {
            // Arrange
            var tempFilePath = Path.GetTempFileName();
            try
            {
                // Create a YAML file with empty connections
                await File.WriteAllTextAsync(tempFilePath, "connections: []");

                // Act
                var changes = await orchestrator.GenerateChangesFromFileAsync(tempFilePath);

                // Assert
                Assert.Empty(changes);
            }
            finally
            {
                File.Delete(tempFilePath);
            }
        }

        [Fact]
        public async Task GenerateChangesFromFileAsync_VerifyLoggingCalled()
        {
            // Arrange
            var yamlFilePath = Path.Combine("DemoData", "ClusterScopedChanges", "multipleClusters.yml");
            
            // Set up a simple mock for the clusters
            var kustoHandler1Mock = new Mock<KustoClusterHandler>(new Mock<KustoClient>("test1.eastus").Object, new Mock<ILogger<KustoClusterHandler>>().Object, "test1", "test1.eastus");
            var kustoHandler2Mock = new Mock<KustoClusterHandler>(new Mock<KustoClient>("test2.eastus").Object, new Mock<ILogger<KustoClusterHandler>>().Object, "test2", "test2.eastus");
            
            kustoClusterHandlerFactoryMock
                .Setup(f => f.Create("test1", "test1.eastus"))
                .Returns(kustoHandler1Mock.Object);
            kustoClusterHandlerFactoryMock
                .Setup(f => f.Create("test2", "test2.eastus"))
                .Returns(kustoHandler2Mock.Object);
            
            kustoHandler1Mock
                .Setup(h => h.LoadAsync())
                .ReturnsAsync(new Cluster { Name = "test1" });
            kustoHandler2Mock
                .Setup(h => h.LoadAsync())
                .ReturnsAsync(new Cluster { Name = "test2" });

            // Act
            await orchestrator.GenerateChangesFromFileAsync(yamlFilePath);

            // Assert - Verify that loading logging was called
            loggerMock.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Loading cluster configurations from file")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);

            // Verify that loaded count logging was called
            loggerMock.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Loaded 2 cluster configuration(s) from YAML file")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);

            // Verify that individual cluster processing logging was called
            loggerMock.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Generating cluster diff for test1")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);

            loggerMock.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Generating cluster diff for test2")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                Times.Once);
        }

        [Fact]
        public async Task GenerateChangesFromFileAsync_LoadAsyncThrowsException_PropagatesException()
        {
            // Arrange
            var yamlFilePath = Path.Combine("DemoData", "ClusterScopedChanges", "multipleClusters.yml");
            
            var kustoHandler1Mock = new Mock<KustoClusterHandler>(new Mock<KustoClient>("test1.eastus").Object, new Mock<ILogger<KustoClusterHandler>>().Object, "test1", "test1.eastus");
            
            kustoClusterHandlerFactoryMock
                .Setup(f => f.Create("test1", "test1.eastus"))
                .Returns(kustoHandler1Mock.Object);
            
            kustoHandler1Mock
                .Setup(h => h.LoadAsync())
                .ThrowsAsync(new Exception("Kusto connection failed"));

            // Act & Assert
            var exception = await Assert.ThrowsAsync<Exception>(() => orchestrator.GenerateChangesFromFileAsync(yamlFilePath));
            Assert.Equal("Kusto connection failed", exception.Message);
        }
    }
}
