using FluentAssertions;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Model
{
    public class UpdatePolicyModelTests
    {
        [Fact]
        public void UpdatePolicy_Should_Initialize_With_Default_Values()
        {
            // Act
            var updatePolicy = new UpdatePolicy();

            // Assert
            updatePolicy.Source.Should().BeNull();
            updatePolicy.Query.Should().BeNull();
            updatePolicy.ManagedIdentity.Should().BeNull();
            updatePolicy.IsEnabled.Should().BeTrue();
            updatePolicy.IsTransactional.Should().BeFalse();
            updatePolicy.PropagateIngestionProperties.Should().BeTrue(); // Default is true
        }

        [Fact]
        public void UpdatePolicy_Should_Allow_Property_Assignment()
        {
            // Arrange
            var updatePolicy = new UpdatePolicy();

            // Act
            updatePolicy.Source = "SourceTable";
            updatePolicy.Query = "SourceTable | extend ProcessedTime = now()";
            updatePolicy.ManagedIdentity = "system";
            updatePolicy.IsEnabled = false;
            updatePolicy.IsTransactional = true;
            updatePolicy.PropagateIngestionProperties = true;

            // Assert
            updatePolicy.Source.Should().Be("SourceTable");
            updatePolicy.Query.Should().Be("SourceTable | extend ProcessedTime = now()");
            updatePolicy.ManagedIdentity.Should().Be("system");
            updatePolicy.IsEnabled.Should().BeFalse();
            updatePolicy.IsTransactional.Should().BeTrue();
            updatePolicy.PropagateIngestionProperties.Should().BeTrue();
        }

        #region UpdatePolicyValidator Tests

        [Fact]
        public void ValidatePolicy_Should_Return_Error_When_UpdatePolicy_Is_Null()
        {
            // Arrange
            var targetTable = CreateTestTable("TargetTable");
            var database = CreateTestDatabase();

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(null!, targetTable, null, database);

            // Assert
            result.IsValid.Should().BeFalse();
            result.Errors.Should().Contain("UpdatePolicy cannot be null");
        }

        [Fact]
        public void ValidatePolicy_Should_Return_Error_When_TargetTable_Is_Null()
        {
            // Arrange
            var updatePolicy = CreateTestUpdatePolicy();
            var database = CreateTestDatabase();

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, null!, null, database);

            // Assert
            result.IsValid.Should().BeFalse();
            result.Errors.Should().Contain("Target table cannot be null");
        }

        [Fact]
        public void ValidatePolicy_Should_Return_Error_When_Source_Is_Empty()
        {
            // Arrange
            var updatePolicy = new UpdatePolicy { Source = "", Query = "TestTable | project *" };
            var targetTable = CreateTestTable("TargetTable");
            var database = CreateTestDatabase();

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, null, database);

            // Assert
            result.IsValid.Should().BeFalse();
            result.Errors.Should().Contain("UpdatePolicy.Source cannot be null or empty");
        }

        [Fact]
        public void ValidatePolicy_Should_Return_Error_When_Query_Is_Empty()
        {
            // Arrange
            var updatePolicy = new UpdatePolicy { Source = "SourceTable", Query = "" };
            var targetTable = CreateTestTable("TargetTable");
            var database = CreateTestDatabase();

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, null, database);

            // Assert
            result.IsValid.Should().BeFalse();
            result.Errors.Should().Contain("UpdatePolicy.Query cannot be null or empty");
        }

        [Fact]
        public void ValidatePolicy_Should_Return_Error_When_Source_Table_Does_Not_Exist()
        {
            // Arrange
            var updatePolicy = CreateTestUpdatePolicy("NonExistentTable");
            var targetTable = CreateTestTable("TargetTable");
            var database = CreateTestDatabase(); // Only contains "SourceTable"

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, null, database);

            // Assert
            result.IsValid.Should().BeFalse();
            result.Errors.Should().Contain("Source table 'NonExistentTable' does not exist in the database");
        }

        [Fact]
        public void ValidatePolicy_Should_Be_Valid_When_All_Conditions_Are_Met()
        {
            // Arrange
            var updatePolicy = CreateTestUpdatePolicy();
            var targetTable = CreateTestTable("TargetTable");
            var sourceTable = CreateTestTable("SourceTable");
            var database = CreateTestDatabase();

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);

            // Assert
            result.IsValid.Should().BeTrue();
            result.Errors.Should().BeEmpty();
        }

        [Fact]
        public void ValidatePolicy_Should_Return_Warning_For_Invalid_ManagedIdentity_Format()
        {
            // Arrange
            var updatePolicy = CreateTestUpdatePolicy();
            updatePolicy.ManagedIdentity = "invalid-format!@#";
            var targetTable = CreateTestTable("TargetTable");
            var sourceTable = CreateTestTable("SourceTable");
            var database = CreateTestDatabase();

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);

            // Assert
            result.IsValid.Should().BeTrue(); // Still valid, just has warning
            result.HasWarnings.Should().BeTrue();
            result.Warnings.Should().Contain(w => w.Contains("Managed identity"));
        }

        [Fact]
        public void ValidatePolicy_Should_Accept_Valid_ManagedIdentity_System()
        {
            // Arrange
            var updatePolicy = CreateTestUpdatePolicy();
            updatePolicy.ManagedIdentity = "system";
            var targetTable = CreateTestTable("TargetTable");
            var sourceTable = CreateTestTable("SourceTable");
            var database = CreateTestDatabase();

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);

            // Assert
            result.IsValid.Should().BeTrue();
            result.Warnings.Should().NotContain(w => w.Contains("Managed identity"));
        }

        [Fact]
        public void ValidatePolicy_Should_Accept_Valid_ManagedIdentity_GUID()
        {
            // Arrange
            var updatePolicy = CreateTestUpdatePolicy();
            updatePolicy.ManagedIdentity = "12345678-1234-1234-1234-123456789012";
            var targetTable = CreateTestTable("TargetTable");
            var sourceTable = CreateTestTable("SourceTable");
            var database = CreateTestDatabase();

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);

            // Assert
            result.IsValid.Should().BeTrue();
            result.Warnings.Should().NotContain(w => w.Contains("Managed identity"));
        }

        [Fact]
        public void ValidatePolicy_Should_Detect_Column_Type_Mismatch()
        {
            // Arrange
            var updatePolicy = new UpdatePolicy
            {
                Source = "SourceTable",
                Query = "SourceTable | project EventId = tostring(123)" // Creates string instead of expected int
            };
            
            var targetTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "int" } // Expects int
                }
            };
            
            var sourceTable = CreateTestTable("SourceTable");
            var database = CreateTestDatabase();

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);

            // Assert
            result.IsValid.Should().BeFalse();
            result.Errors.Should().Contain(e => e.Contains("Column 'EventId' type mismatch"));
        }

        [Fact]
        public void ValidatePolicy_Should_Allow_Compatible_Numeric_Types()
        {
            // Arrange
            var updatePolicy = new UpdatePolicy
            {
                Source = "SourceTable",
                Query = "SourceTable | project Count = 123" // int literal
            };
            
            var targetTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "Count", "long" } // long is compatible with int
                }
            };
            
            var sourceTable = CreateTestTable("SourceTable");
            var database = CreateTestDatabase();

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);

            // Assert
            result.IsValid.Should().BeTrue();
        }

        [Fact]
        public void ValidatePolicy_Should_Allow_Dynamic_Type_Compatibility()
        {
            // Arrange
            var updatePolicy = new UpdatePolicy
            {
                Source = "SourceTable",
                Query = "SourceTable | project Data = todynamic('{\"test\": 1}')"
            };
            
            var targetTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "Data", "string" } // dynamic is compatible with any type
                }
            };
            
            var sourceTable = CreateTestTable("SourceTable");
            var database = CreateTestDatabase();

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);

            // Assert
            result.IsValid.Should().BeTrue();
        }

        [Fact]
        public void UpdatePolicyValidationResult_Should_Track_Multiple_Errors_And_Warnings()
        {
            // Arrange
            var result = new UpdatePolicyValidationResult();

            // Act
            result.AddError("Error 1");
            result.AddError("Error 2");
            result.AddWarning("Warning 1");

            // Assert
            result.IsValid.Should().BeFalse();
            result.HasWarnings.Should().BeTrue();
            result.Errors.Should().HaveCount(2);
            result.Warnings.Should().HaveCount(1);
            result.ToString().Should().Contain("Error 1, Error 2");
            result.ToString().Should().Contain("Warning 1");
        }

        [Fact]
        public void UpdatePolicyValidationResult_Should_Report_Valid_When_No_Errors()
        {
            // Arrange
            var result = new UpdatePolicyValidationResult();

            // Act
            result.AddWarning("Just a warning");

            // Assert
            result.IsValid.Should().BeTrue();
            result.HasWarnings.Should().BeTrue();
            result.ToString().Should().Contain("Warning");
        }

        #endregion

        #region Helper Methods

        private static UpdatePolicy CreateTestUpdatePolicy(string sourceName = "SourceTable")
        {
            return new UpdatePolicy
            {
                Source = sourceName,
                Query = $"{sourceName} | extend ProcessedTime = now()"
            };
        }

        private static Table CreateTestTable(string name)
        {
            return new Table
            {
                Columns = name == "TargetTable" ? 
                    new Dictionary<string, string>
                    {
                        { "EventId", "string" },
                        { "Timestamp", "datetime" },
                        { "Data", "dynamic" },
                        { "ProcessedTime", "datetime" } // Add ProcessedTime to target table
                    } :
                    new Dictionary<string, string>
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
