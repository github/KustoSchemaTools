using FluentAssertions;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Model
{
    public class PolicyModelTests
    {
        [Fact]
        public void TablePolicy_ValidateUpdatePolicies_Should_Return_Empty_When_No_Update_Policies()
        {
            // Arrange
            var tablePolicy = new TablePolicy();
            var targetTable = CreateTestTable("TargetTable");
            var database = CreateTestDatabase();

            // Act
            var results = tablePolicy.ValidateUpdatePolicies(targetTable, database);

            // Assert
            results.Should().BeEmpty();
        }

        [Fact]
        public void TablePolicy_ValidateUpdatePolicies_Should_Validate_All_Update_Policies()
        {
            // Arrange
            var tablePolicy = new TablePolicy
            {
                UpdatePolicies = new List<UpdatePolicy>
                {
                    new UpdatePolicy { Source = "SourceTable", Query = "SourceTable | project *" },
                    new UpdatePolicy { Source = "NonExistentTable", Query = "NonExistentTable | project *" }
                }
            };
            var targetTable = CreateTestTable("TargetTable");
            var database = CreateTestDatabase();

            // Act
            var results = tablePolicy.ValidateUpdatePolicies(targetTable, database);

            // Assert
            results.Should().HaveCount(2);
            results[0].IsValid.Should().BeTrue(); // First policy is valid
            results[1].IsValid.Should().BeFalse(); // Second policy references non-existent table
            results[1].Errors.Should().Contain(e => e.Contains("NonExistentTable"));
        }

        [Fact]
        public void TablePolicy_CreateScripts_Should_Validate_Update_Policies_When_Requested()
        {
            // Arrange
            var tablePolicy = new TablePolicy
            {
                UpdatePolicies = new List<UpdatePolicy>
                {
                    new UpdatePolicy { Source = "NonExistentTable", Query = "NonExistentTable | project *" }
                }
            };
            var targetTable = CreateTestTable("TargetTable");
            var database = CreateTestDatabase();

            // Act & Assert
            var action = () => tablePolicy.CreateScripts("TargetTable", targetTable, database, validatePolicies: true);
            action.Should().Throw<InvalidOperationException>()
                .WithMessage("*validation failed*")
                .WithMessage("*NonExistentTable*");
        }

        [Fact]
        public void TablePolicy_CreateScripts_Should_Not_Validate_When_Validation_Disabled()
        {
            // Arrange
            var tablePolicy = new TablePolicy
            {
                UpdatePolicies = new List<UpdatePolicy>
                {
                    new UpdatePolicy { Source = "NonExistentTable", Query = "NonExistentTable | project *" }
                }
            };
            var targetTable = CreateTestTable("TargetTable");
            var database = CreateTestDatabase();

            // Act
            var scripts = tablePolicy.CreateScripts("TargetTable", targetTable, database, validatePolicies: false);

            // Assert
            scripts.Should().NotBeEmpty(); // Should generate scripts despite invalid policy
        }

        [Fact]
        public void TablePolicy_CreateScripts_Should_Pass_Validation_With_Valid_Update_Policies()
        {
            // Arrange
            var tablePolicy = new TablePolicy
            {
                UpdatePolicies = new List<UpdatePolicy>
                {
                    new UpdatePolicy { Source = "SourceTable", Query = "SourceTable | extend ProcessedTime = now()" }
                }
            };
            var targetTable = CreateTestTable("TargetTable");
            var database = CreateTestDatabase();

            // Act
            var scripts = tablePolicy.CreateScripts("TargetTable", targetTable, database, validatePolicies: true);

            // Assert
            scripts.Should().NotBeEmpty();
            scripts.Should().Contain(s => s.Kind == "TableUpdatePolicy");
        }

        [Fact]
        public void TablePolicy_Should_Handle_Multiple_Update_Policy_Validation_Errors()
        {
            // Arrange
            var tablePolicy = new TablePolicy
            {
                UpdatePolicies = new List<UpdatePolicy>
                {
                    new UpdatePolicy { Source = "", Query = "InvalidQuery" }, // Empty source
                    new UpdatePolicy { Source = "NonExistentTable", Query = "" } // Empty query
                }
            };
            var targetTable = CreateTestTable("TargetTable");
            var database = CreateTestDatabase();

            // Act & Assert
            var action = () => tablePolicy.CreateScripts("TargetTable", targetTable, database, validatePolicies: true);
            action.Should().Throw<InvalidOperationException>()
                .WithMessage("*validation failed*");
        }

        [Fact]
        public void TablePolicy_Should_Support_Backward_Compatibility_CreateScripts()
        {
            // Arrange
            var tablePolicy = new TablePolicy
            {
                UpdatePolicies = new List<UpdatePolicy>
                {
                    new UpdatePolicy { Source = "SourceTable", Query = "SourceTable | project *" }
                }
            };

            // Act
            var scripts = tablePolicy.CreateScripts("TargetTable");

            // Assert
            scripts.Should().NotBeEmpty();
            scripts.Should().Contain(s => s.Kind == "TableUpdatePolicy");
        }

        #region Helper Methods

        private static Table CreateTestTable(string name)
        {
            return new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" },
                    { "Timestamp", "datetime" },
                    { "Data", "dynamic" }
                }
            };
        }

        private static Database CreateTestDatabase()
        {
            return new Database
            {
                Tables = new Dictionary<string, Table>
                {
                    { "SourceTable", CreateTestTable("SourceTable") },
                    { "TargetTable", CreateTestTable("TargetTable") }
                }
            };
        }

        #endregion
    }
}