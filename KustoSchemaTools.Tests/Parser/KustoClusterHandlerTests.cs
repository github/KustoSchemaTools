using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Moq;
using Kusto.Data.Common;
using System.Data;

namespace KustoSchemaTools.Tests.Parser
{
    public class KustoClusterHandlerTests
    {
        private readonly Mock<ILogger<KustoClusterHandler>> _loggerMock;
        private readonly Mock<ICslAdminProvider> _adminClientMock;
        private readonly KustoClusterHandler _handler;

        public KustoClusterHandlerTests()
        {
            _loggerMock = new Mock<ILogger<KustoClusterHandler>>();
            _adminClientMock = new Mock<ICslAdminProvider>();
            
            _handler = new KustoClusterHandler(
                _adminClientMock.Object,
                _loggerMock.Object,
                "test-cluster",
                "test.eastus"
            );
        }

        [Fact]
        public async Task WriteAsync_WithEmptyChangeSet_ReturnsEmptyResult()
        {
            // Arrange
            var changeSet = new ClusterChangeSet("test-cluster", new Cluster(), new Cluster());
            
            // Act
            var result = await _handler.WriteAsync(changeSet);
            
            // Assert
            Assert.NotNull(result);
            Assert.Empty(result);
            
            // Verify no commands were executed
            _adminClientMock.Verify(x => x.ExecuteControlCommandAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<ClientRequestProperties>()), Times.Never);
        }

        [Fact]
        public async Task WriteAsync_WithInvalidScripts_SkipsInvalidScripts()
        {
            // Arrange
            var changeSet = CreateChangeSetWithScripts(new[]
            {
                new DatabaseScriptContainer("policy", 0, ".alter cluster policy capacity", false) { IsValid = false },
                new DatabaseScriptContainer("policy", 1, ".show cluster policy capacity", false) { IsValid = true }
            });

            var mockResult = CreateMockDataReader();
            _adminClientMock
                .Setup(x => x.ExecuteControlCommandAsync("", It.IsAny<string>(), It.IsAny<ClientRequestProperties>()))
                .ReturnsAsync(mockResult.Object);

            // Act
            var result = await _handler.WriteAsync(changeSet);

            // Assert
            Assert.NotNull(result);
            
            // Verify only valid scripts were included in the execution
            _adminClientMock.Verify(x => x.ExecuteControlCommandAsync(
                "",
                It.Is<string>(cmd => cmd.Contains(".show cluster policy capacity") && !cmd.Contains(".alter cluster policy capacity")),
                It.IsAny<ClientRequestProperties>()), Times.Once);
        }

        [Fact]
        public async Task WriteAsync_WithNegativeOrderScripts_SkipsNegativeOrderScripts()
        {
            // Arrange
            var changeSet = CreateChangeSetWithScripts(new[]
            {
                new DatabaseScriptContainer("policy", -1, ".alter cluster policy capacity", false) { IsValid = true },
                new DatabaseScriptContainer("policy", 0, ".show cluster policy capacity", false) { IsValid = true }
            });

            var mockResult = CreateMockDataReader();
            _adminClientMock
                .Setup(x => x.ExecuteControlCommandAsync("", It.IsAny<string>(), It.IsAny<ClientRequestProperties>()))
                .ReturnsAsync(mockResult.Object);

            // Act
            var result = await _handler.WriteAsync(changeSet);

            // Assert
            Assert.NotNull(result);
            
            // Verify negative order scripts were excluded
            _adminClientMock.Verify(x => x.ExecuteControlCommandAsync(
                "",
                It.Is<string>(cmd => cmd.Contains(".show cluster policy capacity") && !cmd.Contains(".alter cluster policy capacity")),
                It.IsAny<ClientRequestProperties>()), Times.Once);
        }

        [Fact]
        public async Task WriteAsync_WithMultipleValidScripts_ExecutesInCorrectOrder()
        {
            // Arrange
            var changeSet = CreateChangeSetWithScripts(new[]
            {
                new DatabaseScriptContainer("policy", 2, ".alter cluster policy capacity script2", false) { IsValid = true },
                new DatabaseScriptContainer("policy", 0, ".alter cluster policy capacity script0", false) { IsValid = true },
                new DatabaseScriptContainer("policy", 1, ".alter cluster policy capacity script1", false) { IsValid = true }
            });

            var mockResult = CreateMockDataReader();
            _adminClientMock
                .Setup(x => x.ExecuteControlCommandAsync("", It.IsAny<string>(), It.IsAny<ClientRequestProperties>()))
                .ReturnsAsync(mockResult.Object);

            // Act
            var result = await _handler.WriteAsync(changeSet);

            // Assert
            Assert.NotNull(result);
            
            // Verify scripts were executed in order (script0, script1, script2)
            _adminClientMock.Verify(x => x.ExecuteControlCommandAsync(
                "",
                It.Is<string>(cmd => 
                    cmd.Contains("script0") && 
                    cmd.Contains("script1") && 
                    cmd.Contains("script2") &&
                    cmd.IndexOf("script0") < cmd.IndexOf("script1") &&
                    cmd.IndexOf("script1") < cmd.IndexOf("script2")),
                It.IsAny<ClientRequestProperties>()), Times.Once);
        }

        [Fact]
        public async Task WriteAsync_WithValidScripts_GeneratesCorrectClusterScript()
        {
            // Arrange
            var changeSet = CreateChangeSetWithScripts(new[]
            {
                new DatabaseScriptContainer("policy", 0, ".alter cluster policy capacity", false) { IsValid = true },
                new DatabaseScriptContainer("policy", 1, ".show cluster policy capacity", false) { IsValid = true }
            });

            var mockResult = CreateMockDataReader();
            _adminClientMock
                .Setup(x => x.ExecuteControlCommandAsync("", It.IsAny<string>(), It.IsAny<ClientRequestProperties>()))
                .ReturnsAsync(mockResult.Object);

            // Act
            var result = await _handler.WriteAsync(changeSet);

            // Assert
            Assert.NotNull(result);
            
            // Verify the correct cluster script format was generated
            _adminClientMock.Verify(x => x.ExecuteControlCommandAsync(
                "",
                It.Is<string>(cmd => 
                    cmd.StartsWith(".execute cluster script with(ContinueOnErrors = true) <|") &&
                    cmd.Contains(".alter cluster policy capacity") &&
                    cmd.Contains(".show cluster policy capacity")),
                It.IsAny<ClientRequestProperties>()), Times.Once);
        }

        [Fact]
        public async Task WriteAsync_WithMixedValidityAndOrder_FiltersCorrectly()
        {
            // Arrange
            var changeSet = CreateChangeSetWithScripts(new[]
            {
                new DatabaseScriptContainer("policy", -1, "script1", false) { IsValid = true }, // Filtered out (negative order)
                new DatabaseScriptContainer("policy", 0, "script2", false) { IsValid = false }, // Filtered out (invalid)
                new DatabaseScriptContainer("policy", 1, "script3", false) { IsValid = true }, // Should be included
                new DatabaseScriptContainer("policy", 2, "script4", false) { IsValid = null }, // Filtered out (not explicitly valid)
                new DatabaseScriptContainer("policy", 3, "script5", false) { IsValid = true }  // Should be included
            });

            var mockResult = CreateMockDataReader();
            _adminClientMock
                .Setup(x => x.ExecuteControlCommandAsync("", It.IsAny<string>(), It.IsAny<ClientRequestProperties>()))
                .ReturnsAsync(mockResult.Object);

            // Act
            var result = await _handler.WriteAsync(changeSet);

            // Assert
            Assert.NotNull(result);
            
            // Verify only script3 and script5 were included
            _adminClientMock.Verify(x => x.ExecuteControlCommandAsync(
                "",
                It.Is<string>(cmd => 
                    cmd.Contains("script3") &&
                    cmd.Contains("script5") &&
                    !cmd.Contains("script1") &&
                    !cmd.Contains("script2") &&
                    !cmd.Contains("script4")),
                It.IsAny<ClientRequestProperties>()), Times.Once);
        }

        [Fact]
        public async Task WriteAsync_CallsExecuteControlCommandAsync()
        {
            // Arrange
            var changeSet = CreateChangeSetWithScripts(new[]
            {
                new DatabaseScriptContainer("policy", 0, ".alter cluster policy capacity", false) { IsValid = true }
            });

            var mockResult = CreateMockDataReader();
            _adminClientMock
                .Setup(x => x.ExecuteControlCommandAsync("", It.IsAny<string>(), It.IsAny<ClientRequestProperties>()))
                .ReturnsAsync(mockResult.Object);

            // Act
            var result = await _handler.WriteAsync(changeSet);

            // Assert
            Assert.NotNull(result);
            _adminClientMock.Verify(x => x.ExecuteControlCommandAsync(
                "",
                It.IsAny<string>(),
                It.IsAny<ClientRequestProperties>()), Times.Once);
        }

        [Fact]
        public async Task LoadAsync_WithCapacityPolicy_ReturnsClusterWithPolicy()
        {
            // Arrange
            var policyJson = """
            {
                "IngestionCapacity": {
                    "ClusterMaximumConcurrentOperations": 500,
                    "CoreUtilizationCoefficient": 0.75
                },
                "MaterializedViewsCapacity": {
                    "ClusterMinimumConcurrentOperations": 10,
                    "ClusterMaximumConcurrentOperations": 100
                },
                "MirroringCapacity": {
                    "ClusterMaximumConcurrentOperations": 50
                }
            }
            """;

            var mockReader = new Mock<IDataReader>();
            mockReader.SetupSequence(x => x.Read())
                .Returns(true)   // First call returns true (data available)
                .Returns(false); // Second call returns false (no more data)
            mockReader.Setup(x => x["Policy"]).Returns(policyJson);

            _adminClientMock
                .Setup(x => x.ExecuteControlCommandAsync("", ".show cluster policy capacity", It.IsAny<ClientRequestProperties>()))
                .ReturnsAsync(mockReader.Object);

            // Act
            var result = await _handler.LoadAsync();

            // Assert
            Assert.NotNull(result);
            Assert.Equal("test-cluster", result.Name);
            Assert.Equal("test.eastus", result.Url);
            Assert.NotNull(result.CapacityPolicy);
            
            Assert.NotNull(result.CapacityPolicy.IngestionCapacity);
            Assert.Equal(500, result.CapacityPolicy.IngestionCapacity.ClusterMaximumConcurrentOperations);
            Assert.Equal(0.75, result.CapacityPolicy.IngestionCapacity.CoreUtilizationCoefficient);

            Assert.NotNull(result.CapacityPolicy.MaterializedViewsCapacity);
            Assert.Equal(10, result.CapacityPolicy.MaterializedViewsCapacity.ClusterMinimumConcurrentOperations);
            Assert.Equal(100, result.CapacityPolicy.MaterializedViewsCapacity.ClusterMaximumConcurrentOperations);

            Assert.NotNull(result.CapacityPolicy.MirroringCapacity);
            Assert.Equal(50, result.CapacityPolicy.MirroringCapacity.ClusterMaximumConcurrentOperations);

            // Verify the correct command was executed
            _adminClientMock.Verify(x => x.ExecuteControlCommandAsync(
                "",
                ".show cluster policy capacity",
                It.IsAny<ClientRequestProperties>()), Times.Once);
        }

        [Fact]
        public async Task LoadAsync_WithNoPolicyData_ReturnsClusterWithoutPolicy()
        {
            // Arrange
            var mockReader = new Mock<IDataReader>();
            mockReader.Setup(x => x.Read()).Returns(false); // No data

            _adminClientMock
                .Setup(x => x.ExecuteControlCommandAsync("", ".show cluster policy capacity", It.IsAny<ClientRequestProperties>()))
                .ReturnsAsync(mockReader.Object);

            // Act
            var result = await _handler.LoadAsync();

            // Assert
            Assert.NotNull(result);
            Assert.Equal("test-cluster", result.Name);
            Assert.Equal("test.eastus", result.Url);
            Assert.Null(result.CapacityPolicy);

            // Verify the correct command was executed
            _adminClientMock.Verify(x => x.ExecuteControlCommandAsync(
                "",
                ".show cluster policy capacity",
                It.IsAny<ClientRequestProperties>()), Times.Once);
        }

        [Fact]
        public async Task LoadAsync_WithEmptyPolicyJson_ReturnsClusterWithoutPolicy()
        {
            // Arrange
            var mockReader = new Mock<IDataReader>();
            mockReader.SetupSequence(x => x.Read())
                .Returns(true)   // First call returns true (data available)
                .Returns(false); // Second call returns false (no more data)
            mockReader.Setup(x => x["Policy"]).Returns(""); // Empty policy

            _adminClientMock
                .Setup(x => x.ExecuteControlCommandAsync("", ".show cluster policy capacity", It.IsAny<ClientRequestProperties>()))
                .ReturnsAsync(mockReader.Object);

            // Act
            var result = await _handler.LoadAsync();

            // Assert
            Assert.NotNull(result);
            Assert.Equal("test-cluster", result.Name);
            Assert.Equal("test.eastus", result.Url);
            Assert.Null(result.CapacityPolicy);
        }

        [Fact]
        public async Task LoadAsync_WithNullPolicyJson_ReturnsClusterWithoutPolicy()
        {
            // Arrange
            var mockReader = new Mock<IDataReader>();
            mockReader.SetupSequence(x => x.Read())
                .Returns(true)   // First call returns true (data available)
                .Returns(false); // Second call returns false (no more data)
            mockReader.Setup(x => x["Policy"]).Returns((object?)null); // Null policy

            _adminClientMock
                .Setup(x => x.ExecuteControlCommandAsync("", ".show cluster policy capacity", It.IsAny<ClientRequestProperties>()))
                .ReturnsAsync(mockReader.Object);

            // Act
            var result = await _handler.LoadAsync();

            // Assert
            Assert.NotNull(result);
            Assert.Equal("test-cluster", result.Name);
            Assert.Equal("test.eastus", result.Url);
            Assert.Null(result.CapacityPolicy);
        }

        #region Helper Methods

        private ClusterChangeSet CreateChangeSetWithScripts(DatabaseScriptContainer[] scripts)
        {
            var changeSet = new ClusterChangeSet("test-cluster", new Cluster(), new Cluster());
            
            // Create a mock change that contains the scripts
            var mockChange = new Mock<IChange>();
            mockChange.Setup(x => x.Scripts).Returns(scripts.ToList());
            
            changeSet.Changes.Add(mockChange.Object);
            
            return changeSet;
        }

        private Mock<IDataReader> CreateMockDataReader()
        {
            var mockReader = new Mock<IDataReader>();
            
            // Make Read() return false to simulate no data
            mockReader.Setup(x => x.Read()).Returns(false);

            return mockReader;
        }

        #endregion
    }
}
