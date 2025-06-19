using FluentAssertions;
using KustoSchemaTools.Model;
using KustoSchemaTools.Changes;

namespace KustoSchemaTools.Tests.Model
{
    public class TableModelTests
    {
        [Fact]
        public void Table_Should_Initialize_With_Default_Values()
        {
            // Act
            var table = new Table();

            // Assert
            table.Folder.Should().BeNull();
            table.DocString.Should().BeNull();
            table.Policies.Should().BeNull();
            table.Columns.Should().BeNull();
            table.Scripts.Should().BeNull();
        }

        [Fact]
        public void Table_Should_Allow_Property_Assignment()
        {
            // Arrange
            var table = new Table();
            var policy = new TablePolicy();

            // Act
            table.Folder = "TestFolder";
            table.DocString = "Test documentation";
            table.Policies = policy;
            table.Columns = new Dictionary<string, string>();
            table.Columns.Add("TestColumn", "string");

            // Assert
            table.Folder.Should().Be("TestFolder");
            table.DocString.Should().Be("Test documentation");
            table.Policies.Should().Be(policy);
            table.Columns.Should().ContainKey("TestColumn").WhoseValue.Should().Be("string");
        }

        [Fact]
        public void Table_Should_Generate_Creation_Scripts()
        {
            // Arrange
            var table = new Table
            {
                Folder = "TestFolder",
                DocString = "Test table",
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" },
                    { "Timestamp", "datetime" }
                }
            };

            // Act
            var scripts = table.CreateScripts("TestTable", true);

            // Assert
            scripts.Should().NotBeEmpty();
            var createScript = scripts.FirstOrDefault(s => s.Kind == "CreateMergeTable");
            createScript.Should().NotBeNull();
            createScript!.Script.Text.Should().Contain(".create-merge table TestTable");
            createScript.Script.Text.Should().Contain("EventId:string");
            createScript.Script.Text.Should().Contain("Timestamp:datetime");
        }

        [Fact]
        public void Table_CreateScripts_Should_Validate_Update_Policies_When_Requested()
        {
            // Arrange
            var table = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" },
                    { "Timestamp", "datetime" }
                },
                Policies = new TablePolicy
                {
                    UpdatePolicies = new List<UpdatePolicy>
                    {
                        new UpdatePolicy { Source = "NonExistentTable", Query = "NonExistentTable | project *" }
                    }
                }
            };
            var database = CreateTestDatabase();

            // Act & Assert
            var action = () => table.CreateScripts("TestTable", true, database, validateUpdatePolicies: true);
            action.Should().Throw<InvalidOperationException>()
                .WithMessage("*validation failed*")
                .WithMessage("*NonExistentTable*");
        }

        [Fact]
        public void Table_CreateScripts_Should_Pass_With_Valid_Update_Policies()
        {
            // Arrange
            var table = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" },
                    { "Timestamp", "datetime" }
                },
                Policies = new TablePolicy
                {
                    UpdatePolicies = new List<UpdatePolicy>
                    {
                        new UpdatePolicy { Source = "SourceTable", Query = "SourceTable | extend ProcessedTime = now()" }
                    }
                }
            };
            var database = CreateTestDatabase();

            // Act
            var scripts = table.CreateScripts("TestTable", true, database, validateUpdatePolicies: true);

            // Assert
            scripts.Should().NotBeEmpty();
            scripts.Should().Contain(s => s.Kind == "TableUpdatePolicy");
        }

        [Fact]
        public void Table_CreateScripts_Should_Not_Validate_When_Validation_Disabled()
        {
            // Arrange
            var table = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" },
                    { "Timestamp", "datetime" }
                },
                Policies = new TablePolicy
                {
                    UpdatePolicies = new List<UpdatePolicy>
                    {
                        new UpdatePolicy { Source = "NonExistentTable", Query = "NonExistentTable | project *" }
                    }
                }
            };
            var database = CreateTestDatabase();

            // Act
            var scripts = table.CreateScripts("TestTable", true, database, validateUpdatePolicies: false);

            // Assert
            scripts.Should().NotBeEmpty(); // Should generate scripts despite invalid policy
        }

        [Fact]
        public void Table_CreateScripts_Should_Support_Backward_Compatibility()
        {
            // Arrange
            var table = new Table
            {
                Columns = new Dictionary<string, string>
                {
                    { "EventId", "string" },
                    { "Timestamp", "datetime" }
                },
                Policies = new TablePolicy
                {
                    UpdatePolicies = new List<UpdatePolicy>
                    {
                        new UpdatePolicy { Source = "SourceTable", Query = "SourceTable | project *" }
                    }
                }
            };

            // Act
            var scripts = table.CreateScripts("TestTable", true);

            // Assert
            scripts.Should().NotBeEmpty();
        }

        #region Helper Methods

        private static Database CreateTestDatabase()
        {
            return new Database
            {
                Tables = new Dictionary<string, Table>
                {
                    { "SourceTable", CreateTestTable("SourceTable") },
                    { "TestTable", CreateTestTable("TestTable") }
                }
            };
        }

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

        #endregion
    }
}
