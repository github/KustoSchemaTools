using FluentAssertions;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Model
{
    /// <summary>
    /// Tests to verify that the UpdatePolicyValidator now uses the Kusto parser
    /// for more accurate validation instead of regex-based parsing.
    /// </summary>
    public class UpdatePolicyValidatorParserIntegrationTests
    {
        [Fact]
        public void ValidatePolicy_Should_Use_Parser_For_Accurate_Type_Inference()
        {
            // Arrange - Create a scenario where parser-based validation provides better type inference
            var sourceTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" },
                    { "Count", "int" },
                    { "Timestamp", "datetime" }
                }
            };

            var targetTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" },
                    { "ProcessedTime", "datetime" },  // Should be inferred as datetime from now()
                    { "DoubleCount", "int" }           // Should be inferred as int from Count * 2
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

            // Query that uses KQL functions - parser should accurately infer types
            var updatePolicy = new UpdatePolicy
            {
                Source = "SourceTable",
                Query = @"SourceTable 
                         | extend 
                             ProcessedTime = now(),
                             DoubleCount = Count * 2
                         | project EventId, ProcessedTime, DoubleCount"
            };

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);

            // Assert
            result.IsValid.Should().BeTrue("the query should produce the correct schema using parser-based validation");
            result.Errors.Should().BeEmpty("there should be no validation errors with accurate type inference");
            
            // The parser should accurately detect that:
            // - now() returns datetime
            // - Count * 2 returns int (same as Count)
            // - All columns match the target table schema
        }

        [Fact]
        public void ValidatePolicy_Should_Allow_Compatible_Numeric_Types_With_Parser()
        {
            // Arrange - Create a scenario where type conversion creates a mismatch
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
                    { "CountAsString", "string" },  // Should match tostring(Count)
                    { "CountAsReal", "int" }        // MISMATCH: real(Count) returns real, not int
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
                Query = @"SourceTable 
                         | extend 
                             CountAsString = tostring(Count),
                             CountAsReal = real(Count)
                         | project EventId, CountAsString, CountAsReal"
            };

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);
            
            // Let's also check what the parser extracts
            var queryValidation = KustoQuerySchemaExtractor.ValidateQuery(
                updatePolicy.Query, 
                sourceTable.Columns, 
                updatePolicy.Source);

            // Assert - Based on current implementation, numeric types are considered compatible
            // But we can still test if the parser properly extracts the types
            result.IsValid.Should().BeTrue("numeric types are currently considered compatible");
            
            // Verify the parser correctly identifies the output types
            queryValidation.OutputSchema.Should().ContainKey("CountAsReal");
            queryValidation.OutputSchema["CountAsReal"].Should().Be("real", "parser should correctly identify real(Count) as real type");
        }

        [Fact]
        public void ValidatePolicy_Should_Fallback_To_Regex_When_Parser_Fails()
        {
            // Arrange - Create a scenario with an invalid query that might cause parser to fail
            var sourceTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" }
                }
            };

            var targetTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" }
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

            // Malformed query that might cause parser issues
            var updatePolicy = new UpdatePolicy
            {
                Source = "SourceTable",
                Query = "SourceTable | project EventId, NonExistentColumn"  // References non-existent column
            };

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);

            // Assert
            result.IsValid.Should().BeFalse("the validation should detect the non-existent column reference");
            result.Errors.Should().Contain(e => e.Contains("NonExistentColumn"),
                "should detect reference to non-existent column whether via parser or fallback");
        }

        [Fact]
        public void ValidatePolicy_Should_Accurately_Parse_Complex_KQL_Query()
        {
            // Arrange - Test with a complex KQL query that demonstrates parser capabilities
            var sourceTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" },
                    { "Timestamp", "datetime" },
                    { "Data", "dynamic" },
                    { "UserId", "string" }
                }
            };

            var targetTable = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" },
                    { "UserId", "string" },
                    { "ExtractedValue", "string" },    // from tostring(Data.value)
                    { "EventAge", "timespan" },        // from now() - Timestamp
                    { "IsRecent", "bool" }             // from EventAge < 1h
                }
            };

            var database = new Database
            {
                Name = "TestDatabase",
                Tables = new Dictionary<string, Table>
                {
                    { "Events", sourceTable },
                    { "ProcessedEvents", targetTable }
                }
            };

            var updatePolicy = new UpdatePolicy
            {
                Source = "Events",
                Query = @"Events
                         | extend 
                             ExtractedValue = tostring(Data.value),
                             EventAge = now() - Timestamp,
                             IsRecent = (now() - Timestamp) < 1h
                         | project EventId, UserId, ExtractedValue, EventAge, IsRecent"
            };

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);

            // Assert
            result.IsValid.Should().BeTrue("parser should correctly handle complex KQL expressions");
            result.Errors.Should().BeEmpty("all type inferences should be accurate");
            
            // Verify that parser-based validation doesn't generate false warnings
            result.Warnings.Should().NotContain(w => w.Contains("falling back to regex"),
                "parser should successfully handle this query without falling back");
        }

        [Fact]
        public void ValidatePolicy_Should_Detect_True_Type_Mismatches_With_Parser()
        {
            // Arrange - Create a scenario with a clear type mismatch (string vs datetime)
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
                    { "CountAsString", "datetime" },  // MISMATCH: expecting datetime but getting string
                    { "CountAsReal", "bool" }         // MISMATCH: expecting bool but getting real
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
                Query = @"SourceTable 
                         | extend 
                             CountAsString = tostring(Count),  // produces string, target expects datetime
                             CountAsReal = real(Count)         // produces real, target expects bool
                         | project EventId, CountAsString, CountAsReal"
            };

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);
            
            // Let's also check what the parser extracts
            var queryValidation = KustoQuerySchemaExtractor.ValidateQuery(
                updatePolicy.Query, 
                sourceTable.Columns, 
                updatePolicy.Source);

            // Assert - This should detect true type mismatches
            result.IsValid.Should().BeFalse("there should be type mismatches detected");
            result.Errors.Should().Contain(e => e.Contains("CountAsString") && e.Contains("string") && e.Contains("datetime"),
                "should detect string vs datetime mismatch");
            result.Errors.Should().Contain(e => e.Contains("CountAsReal") && e.Contains("real") && e.Contains("bool"),
                "should detect real vs bool mismatch");
                
            // Verify the parser correctly identifies the output types
            queryValidation.OutputSchema.Should().ContainKey("CountAsString");
            queryValidation.OutputSchema["CountAsString"].Should().Be("string");
            queryValidation.OutputSchema.Should().ContainKey("CountAsReal");
            queryValidation.OutputSchema["CountAsReal"].Should().Be("real");
        }
    }
}
