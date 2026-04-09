using KustoSchemaTools.Model;
using KustoSchemaTools.Changes;
using Microsoft.Extensions.Logging;
using Moq;

namespace KustoSchemaTools.Tests.ManagedIdentity
{
    public class ManagedIdentityPolicyTests
    {
        [Fact]
        public void CreateScript_SingleUsage_GeneratesCorrectKql()
        {
            // Arrange
            var policy = new ManagedIdentityPolicy
            {
                ObjectId = "12345678-1234-1234-1234-123456789abc",
                AllowedUsages = new List<string> { "NativeIngestion" }
            };

            // Act
            var script = policy.CreateScript("MyDatabase");

            // Assert
            Assert.Equal("ManagedIdentityPolicy", script.Kind);
            Assert.Equal(80, script.Script.Order);
            Assert.Contains(".alter-merge database MyDatabase policy managed_identity", script.Script.Text);
            Assert.Contains("\"ObjectId\": \"12345678-1234-1234-1234-123456789abc\"", script.Script.Text);
            Assert.Contains("\"AllowedUsages\": \"NativeIngestion\"", script.Script.Text);
        }

        [Fact]
        public void CreateScript_MultipleUsages_JoinsWithComma()
        {
            // Arrange
            var policy = new ManagedIdentityPolicy
            {
                ObjectId = "12345678-1234-1234-1234-123456789abc",
                AllowedUsages = new List<string> { "AutomatedFlows", "ExternalTable", "NativeIngestion" }
            };

            // Act
            var script = policy.CreateScript("MyDatabase");

            // Assert
            Assert.Contains("\"AllowedUsages\": \"AutomatedFlows, ExternalTable, NativeIngestion\"", script.Script.Text);
        }

        [Fact]
        public void CreateScript_DatabaseNameUsedInKql()
        {
            // Arrange
            var policy = new ManagedIdentityPolicy
            {
                ObjectId = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                AllowedUsages = new List<string> { "ExternalTable" }
            };

            // Act
            var script = policy.CreateScript("TargetDatabase");

            // Assert
            Assert.StartsWith(".alter-merge database TargetDatabase policy managed_identity", script.Script.Text);
        }

        [Fact]
        public void CreateScript_WrapsJsonInBackticks()
        {
            // Arrange
            var policy = new ManagedIdentityPolicy
            {
                ObjectId = "12345678-1234-1234-1234-123456789abc",
                AllowedUsages = new List<string> { "NativeIngestion" }
            };

            // Act
            var script = policy.CreateScript("MyDatabase");

            // Assert
            Assert.Contains("```", script.Script.Text);
            Assert.EndsWith("```", script.Script.Text);
        }

        [Fact]
        public void DatabaseChanges_WithManagedIdentityPolicies_GeneratesScript()
        {
            // Arrange
            var loggerMock = new Mock<ILogger>();
            var oldState = new Database { Name = "TestDb" };
            var newState = new Database
            {
                Name = "TestDb",
                ManagedIdentityPolicies = new List<ManagedIdentityPolicy>
                {
                    new ManagedIdentityPolicy
                    {
                        ObjectId = "12345678-1234-1234-1234-123456789abc",
                        AllowedUsages = new List<string> { "NativeIngestion" }
                    }
                }
            };

            // Act
            var changes = DatabaseChanges.GenerateChanges(oldState, newState, "TestDb", loggerMock.Object);

            // Assert
            Assert.NotEmpty(changes);
            var scripts = changes.SelectMany(c => c.Scripts).ToList();
            Assert.NotEmpty(scripts);
            var managedIdentityScript = scripts.FirstOrDefault(s => s.Kind == "ManagedIdentityPolicy");
            Assert.NotNull(managedIdentityScript);
            Assert.Contains(".alter-merge database TestDb policy managed_identity", managedIdentityScript.Script.Text);
            Assert.Contains("12345678-1234-1234-1234-123456789abc", managedIdentityScript.Script.Text);
        }

        [Fact]
        public void DatabaseChanges_WithUnchangedManagedIdentityPolicies_GeneratesNoChanges()
        {
            // Arrange
            var loggerMock = new Mock<ILogger>();
            var policy = new ManagedIdentityPolicy
            {
                ObjectId = "12345678-1234-1234-1234-123456789abc",
                AllowedUsages = new List<string> { "NativeIngestion" }
            };
            var oldState = new Database
            {
                Name = "TestDb",
                ManagedIdentityPolicies = new List<ManagedIdentityPolicy> { policy }
            };
            var newState = new Database
            {
                Name = "TestDb",
                ManagedIdentityPolicies = new List<ManagedIdentityPolicy>
                {
                    new ManagedIdentityPolicy
                    {
                        ObjectId = "12345678-1234-1234-1234-123456789abc",
                        AllowedUsages = new List<string> { "NativeIngestion" }
                    }
                }
            };

            // Act
            var changes = DatabaseChanges.GenerateChanges(oldState, newState, "TestDb", loggerMock.Object);

            // Assert - no database-level changes since policies are identical
            var databaseScriptChanges = changes
                .SelectMany(c => c.Scripts)
                .Where(s => s.Kind == "ManagedIdentityPolicy")
                .ToList();
            Assert.Empty(databaseScriptChanges);
        }
    }
}
