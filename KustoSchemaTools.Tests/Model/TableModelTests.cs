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
    }
}
