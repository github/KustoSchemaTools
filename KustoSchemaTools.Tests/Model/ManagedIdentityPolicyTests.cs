using KustoSchemaTools.Model;
using KustoSchemaTools.Changes;
using Microsoft.Extensions.Logging;
using Moq;

namespace KustoSchemaTools.Tests.ManagedIdentity
{
    public class ManagedIdentityPolicyTests
    {
        [Fact]
        public void CreateCombinedScript_SinglePolicy_GeneratesCorrectKql()
        {
            // Arrange
            var policies = new List<ManagedIdentityPolicy>
            {
                new ManagedIdentityPolicy
                {
                    ObjectId = "12345678-1234-1234-1234-123456789abc",
                    AllowedUsages = new List<string> { "NativeIngestion" }
                }
            };

            // Act
            var script = ManagedIdentityPolicy.CreateCombinedScript("MyDatabase", policies);

            // Assert
            Assert.Equal("ManagedIdentityPolicy", script.Kind);
            Assert.Equal(80, script.Script.Order);
            Assert.Contains(".alter-merge database MyDatabase policy managed_identity", script.Script.Text);
            Assert.Contains("\"ObjectId\": \"12345678-1234-1234-1234-123456789abc\"", script.Script.Text);
            Assert.Contains("\"AllowedUsages\": \"NativeIngestion\"", script.Script.Text);
        }

        [Fact]
        public void CreateCombinedScript_MultipleUsages_JoinsWithCommaAlphabetically()
        {
            // Arrange
            var policies = new List<ManagedIdentityPolicy>
            {
                new ManagedIdentityPolicy
                {
                    ObjectId = "12345678-1234-1234-1234-123456789abc",
                    AllowedUsages = new List<string> { "NativeIngestion", "AutomatedFlows", "ExternalTable" }
                }
            };

            // Act
            var script = ManagedIdentityPolicy.CreateCombinedScript("MyDatabase", policies);

            // Assert
            Assert.Contains("\"AllowedUsages\": \"AutomatedFlows, ExternalTable, NativeIngestion\"", script.Script.Text);
        }

        [Fact]
        public void CreateCombinedScript_MultiplePolicies_SortsByObjectId()
        {
            // Arrange
            var policies = new List<ManagedIdentityPolicy>
            {
                new ManagedIdentityPolicy
                {
                    ObjectId = "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz",
                    AllowedUsages = new List<string> { "ExternalTable" }
                },
                new ManagedIdentityPolicy
                {
                    ObjectId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    AllowedUsages = new List<string> { "NativeIngestion" }
                }
            };

            // Act
            var script = ManagedIdentityPolicy.CreateCombinedScript("MyDatabase", policies);

            // Assert - both identities in a single script, sorted by ObjectId
            Assert.Equal("ManagedIdentityPolicy", script.Kind);
            var aIdx = script.Script.Text.IndexOf("aaaaaaaa");
            var zIdx = script.Script.Text.IndexOf("zzzzzzzz");
            Assert.True(aIdx < zIdx, "Policies should be sorted by ObjectId");
        }

        [Fact]
        public void CreateCombinedScript_DatabaseNameUsedInKql()
        {
            // Arrange
            var policies = new List<ManagedIdentityPolicy>
            {
                new ManagedIdentityPolicy
                {
                    ObjectId = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                    AllowedUsages = new List<string> { "ExternalTable" }
                }
            };

            // Act
            var script = ManagedIdentityPolicy.CreateCombinedScript("TargetDatabase", policies);

            // Assert
            Assert.StartsWith(".alter-merge database TargetDatabase policy managed_identity", script.Script.Text);
        }

        [Fact]
        public void CreateCombinedScript_WrapsJsonInBackticks()
        {
            // Arrange
            var policies = new List<ManagedIdentityPolicy>
            {
                new ManagedIdentityPolicy
                {
                    ObjectId = "12345678-1234-1234-1234-123456789abc",
                    AllowedUsages = new List<string> { "NativeIngestion" }
                }
            };

            // Act
            var script = ManagedIdentityPolicy.CreateCombinedScript("MyDatabase", policies);

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
        public void DatabaseChanges_WithMultipleManagedIdentityPolicies_GeneratesSingleScript()
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
                        ObjectId = "aaaaaaaa-1111-2222-3333-444444444444",
                        AllowedUsages = new List<string> { "NativeIngestion" }
                    },
                    new ManagedIdentityPolicy
                    {
                        ObjectId = "bbbbbbbb-1111-2222-3333-444444444444",
                        AllowedUsages = new List<string> { "ExternalTable" }
                    }
                }
            };

            // Act
            var changes = DatabaseChanges.GenerateChanges(oldState, newState, "TestDb", loggerMock.Object);

            // Assert - should generate exactly one ManagedIdentityPolicy script (combined)
            var managedIdentityScripts = changes
                .SelectMany(c => c.Scripts)
                .Where(s => s.Kind == "ManagedIdentityPolicy")
                .ToList();
            Assert.Single(managedIdentityScripts);
            Assert.Contains("aaaaaaaa-1111-2222-3333-444444444444", managedIdentityScripts[0].Script.Text);
            Assert.Contains("bbbbbbbb-1111-2222-3333-444444444444", managedIdentityScripts[0].Script.Text);
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
