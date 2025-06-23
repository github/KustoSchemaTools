using FluentAssertions;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Model
{
    /// <summary>
    /// Tests for the UpdatePolicyValidationConfig feature flag functionality.
    /// </summary>
    public class UpdatePolicyValidationConfigTests
    {
        [Fact]
        public void Default_Config_Should_Allow_Numeric_Type_Conversions()
        {
            // Arrange
            var sourceTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "Count", "int" }
                }
            };

            var targetTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "CountAsReal", "real" }  // Target expects real, query produces real (from int)
                }
            };

            var database = new Database
            {
                Name = "TestDatabase",
                Tables = new Dictionary<string, Table>
                {
                    { "SourceTable", sourceTable },
                    { "TargetTable", targetTable }
                }
            };

            var updatePolicy = new UpdatePolicy
            {
                Source = "SourceTable",
                Query = "SourceTable | project CountAsReal = real(Count)"  // int -> real conversion
            };

            // Act - Using default config (should allow numeric conversions)
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);

            // Assert
            result.IsValid.Should().BeTrue("default config should allow numeric type conversions");
        }

        [Fact]
        public void Strict_Config_Should_Reject_Numeric_Type_Conversions()
        {
            // Arrange
            var sourceTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "Count", "int" }
                }
            };

            var targetTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "CountAsReal", "int" }  // Target expects int, but query produces real
                }
            };

            var database = new Database
            {
                Name = "TestDatabase",
                Tables = new Dictionary<string, Table>
                {
                    { "SourceTable", sourceTable },
                    { "TargetTable", targetTable }
                }
            };

            var updatePolicy = new UpdatePolicy
            {
                Source = "SourceTable",
                Query = "SourceTable | project CountAsReal = real(Count)"  // Produces real, target expects int
            };

            // Act - Using strict config (should reject numeric conversions)
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database, UpdatePolicyValidationConfig.Strict);

            // Assert
            result.IsValid.Should().BeFalse("strict config should reject numeric type conversions");
            result.Errors.Should().Contain(e => e.Contains("CountAsReal") && e.Contains("real") && e.Contains("int"),
                "should report type mismatch between real and int types when strict");
        }

        [Fact]
        public void Both_Configs_Should_Allow_Exact_Type_Matches()
        {
            // Arrange
            var sourceTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" },
                    { "Count", "int" }
                }
            };

            var targetTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" },
                    { "Count", "int" }  // Exact same type
                }
            };

            var database = new Database
            {
                Name = "TestDatabase",
                Tables = new Dictionary<string, Table>
                {
                    { "SourceTable", sourceTable },
                    { "TargetTable", targetTable }
                }
            };

            var updatePolicy = new UpdatePolicy
            {
                Source = "SourceTable",
                Query = "SourceTable | project EventId, Count"
            };

            // Act - Test both configs
            var defaultResult = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);
            var strictResult = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database, UpdatePolicyValidationConfig.Strict);

            // Assert
            defaultResult.IsValid.Should().BeTrue("exact type matches should always be valid");
            strictResult.IsValid.Should().BeTrue("exact type matches should always be valid even in strict mode");
        }

        [Fact]
        public void Both_Configs_Should_Allow_Dynamic_Type_Compatibility()
        {
            // Arrange
            var sourceTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "Data", "dynamic" }
                }
            };

            var targetTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "Data", "string" }  // Dynamic should be compatible with anything
                }
            };

            var database = new Database
            {
                Name = "TestDatabase",
                Tables = new Dictionary<string, Table>
                {
                    { "SourceTable", sourceTable },
                    { "TargetTable", targetTable }
                }
            };

            var updatePolicy = new UpdatePolicy
            {
                Source = "SourceTable",
                Query = "SourceTable | project Data = tostring(Data)"
            };

            // Act - Test both configs
            var defaultResult = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);
            var strictResult = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database, UpdatePolicyValidationConfig.Strict);

            // Assert
            defaultResult.IsValid.Should().BeTrue("dynamic should be compatible with other types");
            strictResult.IsValid.Should().BeTrue("dynamic should be compatible with other types even in strict mode");
        }

        [Fact]
        public void Both_Configs_Should_Reject_Incompatible_Non_Numeric_Types()
        {
            // Arrange
            var sourceTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventTime", "datetime" }
                }
            };

            var targetTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventTime", "string" }  // Completely different types
                }
            };

            var database = new Database
            {
                Name = "TestDatabase",
                Tables = new Dictionary<string, Table>
                {
                    { "SourceTable", sourceTable },
                    { "TargetTable", targetTable }
                }
            };

            var updatePolicy = new UpdatePolicy
            {
                Source = "SourceTable",
                Query = "SourceTable | project EventTime"  // Keeps datetime type
            };

            // Act - Test both configs
            var defaultResult = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);
            var strictResult = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database, UpdatePolicyValidationConfig.Strict);

            // Assert
            defaultResult.IsValid.Should().BeFalse("incompatible non-numeric types should be rejected");
            strictResult.IsValid.Should().BeFalse("incompatible non-numeric types should be rejected in strict mode too");
        }

        [Fact]
        public void Config_Default_Properties_Should_Be_Correct()
        {
            // Act
            var defaultConfig = UpdatePolicyValidationConfig.Default;
            var strictConfig = UpdatePolicyValidationConfig.Strict;

            // Assert
            defaultConfig.EnforceStrictTypeCompatibility.Should().BeFalse("default config should allow implicit numeric conversions");
            strictConfig.EnforceStrictTypeCompatibility.Should().BeTrue("strict config should enforce exact type matching");
        }

        [Fact]
        public void Custom_Config_Should_Work()
        {
            // Arrange
            var customConfig = new UpdatePolicyValidationConfig
            {
                EnforceStrictTypeCompatibility = true  // Custom strict setting
            };

            var sourceTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "Count", "int" }
                }
            };

            var targetTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "CountAsLong", "real" }  // Target expects real, but query produces long
                }
            };

            var database = new Database
            {
                Name = "TestDatabase",
                Tables = new Dictionary<string, Table>
                {
                    { "SourceTable", sourceTable },
                    { "TargetTable", targetTable }
                }
            };

            var updatePolicy = new UpdatePolicy
            {
                Source = "SourceTable",
                Query = "SourceTable | project CountAsLong = long(Count)"  // Produces long, target expects real
            };

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database, customConfig);

            // Assert
            result.IsValid.Should().BeFalse("custom strict config should reject numeric type conversions");
        }
    }
}
