using KustoSchemaTools.Model;
using KustoSchemaTools.Changes;
using Microsoft.Extensions.Logging;
using Moq;

namespace KustoSchemaTools.Tests.ManagedIdentity
{
    public class ManagedIdentityPolicyParseTests
    {
        [Fact]
        public void ParseFromPolicyJson_SinglePolicy_ExtractsObjectIdAndClientId()
        {
            var json = @"[
                {
                    ""ObjectId"": ""8749feae-888c-446b-9f38-26f0c38ba1cd"",
                    ""ClientId"": ""1de2a36c-bba4-4380-be8d-5f400303219b"",
                    ""TenantId"": ""398a6654-997b-47e9-b12b-9515b896b4de"",
                    ""DisplayName"": ""my-identity"",
                    ""IsSystem"": false,
                    ""AllowedUsages"": ""AutomatedFlows""
                }
            ]";

            var result = ManagedIdentityPolicy.ParseFromPolicyJson(json);

            Assert.Single(result);
            Assert.Equal("8749feae-888c-446b-9f38-26f0c38ba1cd", result[0].ObjectId);
            Assert.Equal("1de2a36c-bba4-4380-be8d-5f400303219b", result[0].ClientId);
            Assert.Equal(new List<string> { "AutomatedFlows" }, result[0].AllowedUsages);
        }

        [Fact]
        public void ParseFromPolicyJson_MultipleUsages_SplitsAndSortsAlphabetically()
        {
            var json = @"[
                {
                    ""ObjectId"": ""aaaa"",
                    ""ClientId"": ""bbbb"",
                    ""AllowedUsages"": ""NativeIngestion, AutomatedFlows, ExternalTable""
                }
            ]";

            var result = ManagedIdentityPolicy.ParseFromPolicyJson(json);

            Assert.Equal(new List<string> { "AutomatedFlows", "ExternalTable", "NativeIngestion" }, result[0].AllowedUsages);
        }

        [Fact]
        public void ParseFromPolicyJson_MultiplePolicies_SortsByObjectId()
        {
            var json = @"[
                { ""ObjectId"": ""zzzz"", ""ClientId"": ""c1"", ""AllowedUsages"": ""ExternalTable"" },
                { ""ObjectId"": ""aaaa"", ""ClientId"": ""c2"", ""AllowedUsages"": ""NativeIngestion"" }
            ]";

            var result = ManagedIdentityPolicy.ParseFromPolicyJson(json);

            Assert.Equal(2, result.Count);
            Assert.Equal("aaaa", result[0].ObjectId);
            Assert.Equal("zzzz", result[1].ObjectId);
        }

        [Fact]
        public void ParseFromPolicyJson_EmptyJson_ReturnsEmptyList()
        {
            Assert.Empty(ManagedIdentityPolicy.ParseFromPolicyJson("[]"));
        }

        [Fact]
        public void ParseFromPolicyJson_NullOrWhitespace_ReturnsEmptyList()
        {
            Assert.Empty(ManagedIdentityPolicy.ParseFromPolicyJson(null));
            Assert.Empty(ManagedIdentityPolicy.ParseFromPolicyJson(""));
            Assert.Empty(ManagedIdentityPolicy.ParseFromPolicyJson("  "));
        }

        [Fact]
        public void ParseFromPolicyJson_NullClientId_SetsClientIdToNull()
        {
            var json = @"[{ ""ObjectId"": ""aaaa"", ""AllowedUsages"": ""AutomatedFlows"" }]";

            var result = ManagedIdentityPolicy.ParseFromPolicyJson(json);

            Assert.Single(result);
            Assert.Equal("aaaa", result[0].ObjectId);
            Assert.Null(result[0].ClientId);
        }

        [Fact]
        public void ParseFromPolicyJson_IgnoresExtraFields()
        {
            var json = @"[
                {
                    ""ObjectId"": ""aaaa"",
                    ""ClientId"": ""bbbb"",
                    ""TenantId"": ""tttt"",
                    ""DisplayName"": ""my-identity"",
                    ""IsSystem"": false,
                    ""AllowedUsages"": ""AutomatedFlows""
                }
            ]";

            var result = ManagedIdentityPolicy.ParseFromPolicyJson(json);

            Assert.Single(result);
            Assert.Equal("aaaa", result[0].ObjectId);
            Assert.Equal("bbbb", result[0].ClientId);
        }
    }

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
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy>
                    {
                        new ManagedIdentityPolicy
                        {
                            ObjectId = "12345678-1234-1234-1234-123456789abc",
                            AllowedUsages = new List<string> { "NativeIngestion" }
                        }
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
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy>
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
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy> { policy }
                }
            };
            var newState = new Database
            {
                Name = "TestDb",
                Policies = new DatabasePolicies
                {
                    ManagedIdentity = new List<ManagedIdentityPolicy>
                    {
                        new ManagedIdentityPolicy
                        {
                            ObjectId = "12345678-1234-1234-1234-123456789abc",
                            AllowedUsages = new List<string> { "NativeIngestion" }
                        }
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
