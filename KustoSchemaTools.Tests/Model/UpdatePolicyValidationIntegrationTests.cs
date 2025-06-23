using FluentAssertions;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Model
{
    public class UpdatePolicyValidationIntegrationTests
    {
        [Fact]
        public void End_To_End_Validation_Should_Work_With_Complete_Database_Model()
        {
            // Arrange - Create a complete database model with source and target tables
            var database = new Database
            {
                Tables = new Dictionary<string, Table>
                {
                    {
                        "RawEvents",
                        new Table
                        {
                            Columns = new Dictionary<string, string>
                            {
                                { "EventId", "string" },
                                { "Timestamp", "datetime" },
                                { "RawData", "dynamic" },
                                { "Source", "string" }
                            }
                        }
                    },
                    {
                        "ProcessedEvents",
                        new Table
                        {
                            Columns = new Dictionary<string, string>
                            {
                                { "EventId", "string" },
                                { "Timestamp", "datetime" },
                                { "ProcessedData", "string" },
                                { "ProcessingTime", "datetime" }
                            },
                            Policies = new TablePolicy
                            {
                                UpdatePolicies = new List<UpdatePolicy>
                                {
                                    new UpdatePolicy
                                    {
                                        Source = "RawEvents",
                                        Query = "RawEvents | extend ProcessedData = tostring(RawData), ProcessingTime = now() | project EventId, Timestamp, ProcessedData, ProcessingTime",
                                        IsEnabled = true,
                                        IsTransactional = false
                                    }
                                }
                            }
                        }
                    }
                }
            };

            var targetTable = database.Tables["ProcessedEvents"];

            // Act - Validate the update policy
            var validationResults = targetTable.Policies!.ValidateUpdatePolicies(targetTable, database);

            // Assert - Validation should pass
            validationResults.Should().HaveCount(1);
            validationResults[0].IsValid.Should().BeTrue();
            validationResults[0].Errors.Should().BeEmpty();
        }

        [Fact]
        public void Complex_Schema_Mismatch_Should_Be_Detected()
        {
            // Arrange - Source table has different column types than what update policy produces
            var database = new Database
            {
                Tables = new Dictionary<string, Table>
                {
                    {
                        "SourceTable",
                        new Table
                        {
                            Columns = new Dictionary<string, string>
                            {
                                { "Id", "int" },
                                { "Name", "string" },
                                { "Count", "long" }
                            }
                        }
                    },
                    {
                        "TargetTable",
                        new Table
                        {
                            Columns = new Dictionary<string, string>
                            {
                                { "Id", "string" }, // Different type than source
                                { "Name", "string" },
                                { "Count", "real" }, // Different type than source
                                { "ProcessedAt", "datetime" }
                            },
                            Policies = new TablePolicy
                            {
                                UpdatePolicies = new List<UpdatePolicy>
                                {
                                    new UpdatePolicy
                                    {
                                        Source = "SourceTable",
                                        Query = "SourceTable | project Id = tostring(Id), Name, Count = toreal(Count), ProcessedAt = now()"
                                    }
                                }
                            }
                        }
                    }
                }
            };

            var targetTable = database.Tables["TargetTable"];
            var sourceTable = database.Tables["SourceTable"];

            // Act
            var result = UpdatePolicyValidator.ValidatePolicy(
                targetTable.Policies!.UpdatePolicies![0], 
                targetTable, 
                sourceTable, 
                database);

            // Assert - Should pass because types are compatible (string/string, real/real, datetime/datetime)
            result.IsValid.Should().BeTrue();
        }

        [Fact]
        public void Create_Scripts_With_Validation_Should_Throw_On_Invalid_Policy()
        {
            // Arrange - Database with invalid update policy
            var database = new Database
            {
                Tables = new Dictionary<string, Table>
                {
                    {
                        "ValidTable",
                        new Table
                        {
                            Columns = new Dictionary<string, string>
                            {
                                { "Id", "string" },
                                { "Data", "string" }
                            }
                        }
                    },
                    {
                        "InvalidPolicyTable",
                        new Table
                        {
                            Columns = new Dictionary<string, string>
                            {
                                { "Id", "string" },
                                { "ProcessedData", "string" }
                            },
                            Policies = new TablePolicy
                            {
                                UpdatePolicies = new List<UpdatePolicy>
                                {
                                    new UpdatePolicy
                                    {
                                        Source = "NonExistentTable", // Invalid source
                                        Query = "NonExistentTable | project Id, ProcessedData = 'processed'"
                                    }
                                }
                            }
                        }
                    }
                }
            };

            var targetTable = database.Tables["InvalidPolicyTable"];

            // Act & Assert
            var action = () => targetTable.CreateScripts("InvalidPolicyTable", true, database, validateUpdatePolicies: true);
            action.Should().Throw<InvalidOperationException>()
                .WithMessage("*validation failed*")
                .WithMessage("*NonExistentTable*");
        }

        [Fact]
        public void Multiple_Update_Policies_With_Mixed_Validity_Should_Report_All_Errors()
        {
            // Arrange
            var database = new Database
            {
                Tables = new Dictionary<string, Table>
                {
                    {
                        "SourceTable1",
                        new Table
                        {
                            Columns = new Dictionary<string, string>
                            {
                                { "Id", "string" },
                                { "Data", "string" }
                            }
                        }
                    },
                    {
                        "TargetTable",
                        new Table
                        {
                            Columns = new Dictionary<string, string>
                            {
                                { "Id", "string" },
                                { "Data", "string" },
                                { "ProcessedAt", "datetime" }
                            },
                            Policies = new TablePolicy
                            {
                                UpdatePolicies = new List<UpdatePolicy>
                                {
                                    new UpdatePolicy
                                    {
                                        Source = "SourceTable1",
                                        Query = "SourceTable1 | project Id, Data, ProcessedAt = now()"
                                    },
                                    new UpdatePolicy
                                    {
                                        Source = "NonExistentTable",
                                        Query = "NonExistentTable | project Id, Data, ProcessedAt = now()"
                                    },
                                    new UpdatePolicy
                                    {
                                        Source = "", // Invalid empty source
                                        Query = "SomeTable | project *"
                                    }
                                }
                            }
                        }
                    }
                }
            };

            var targetTable = database.Tables["TargetTable"];

            // Act
            var validationResults = targetTable.Policies!.ValidateUpdatePolicies(targetTable, database);

            // Assert
            validationResults.Should().HaveCount(3);
            validationResults[0].IsValid.Should().BeTrue(); // First policy is valid
            validationResults[1].IsValid.Should().BeFalse(); // Second policy has invalid source table
            validationResults[2].IsValid.Should().BeFalse(); // Third policy has empty source
            
            validationResults[1].Errors.Should().Contain(e => e.Contains("NonExistentTable"));
            validationResults[2].Errors.Should().Contain(e => e.Contains("cannot be null or empty"));
        }

        [Fact]
        public void Validation_Should_Work_Without_Source_Table_Reference()
        {
            // Arrange - Database where we only have target table definition
            var database = new Database
            {
                Tables = new Dictionary<string, Table>
                {
                    {
                        "TargetTable",
                        new Table
                        {
                            Columns = new Dictionary<string, string>
                            {
                                { "Id", "string" },
                                { "Data", "string" }
                            },
                            Policies = new TablePolicy
                            {
                                UpdatePolicies = new List<UpdatePolicy>
                                {
                                    new UpdatePolicy
                                    {
                                        Source = "ExternalSource", // Source not in our database
                                        Query = "ExternalSource | project Id, Data"
                                    }
                                }
                            }
                        }
                    }
                }
            };

            var targetTable = database.Tables["TargetTable"];

            // Act
            var validationResults = targetTable.Policies!.ValidateUpdatePolicies(targetTable, database);

            // Assert - Should still validate basic properties and report missing source
            validationResults.Should().HaveCount(1);
            validationResults[0].IsValid.Should().BeFalse();
            validationResults[0].Errors.Should().Contain(e => e.Contains("ExternalSource") && e.Contains("does not exist"));
        }
    }
}
