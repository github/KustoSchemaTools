using KustoSchemaTools.Changes;
using KustoSchemaTools.Configuration;
using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Moq;

namespace KustoSchemaTools.Tests.Changes
{
    public class DatabaseChangesColumnValidationTests
    {
        private readonly Mock<ILogger> _loggerMock;

        public DatabaseChangesColumnValidationTests()
        {
            _loggerMock = new Mock<ILogger>();
        }

        [Fact]
        public void GenerateChanges_ValidColumnOrder_NoCommentAttached()
        {
            // Arrange
            var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
            var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int"), ("NewCol", "bool") }));
            var settings = ValidationSettings.WithColumnOrderValidation();

            // Act
            var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

            // Assert
            var tableChanges = changes.Where(c => c.EntityType == "Table").ToList();
            Assert.All(tableChanges, change => Assert.Null(change.Comment));
        }

        [Fact]
        public void GenerateChanges_InvalidColumnOrder_CautionCommentWithFailsRollout()
        {
            // Arrange
            var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
            var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int") }));
            var settings = ValidationSettings.WithColumnOrderValidation();

            // Act
            var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

            // Assert
            var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
            Assert.NotNull(tableChange);
            Assert.NotNull(tableChange.Comment);
            Assert.True(tableChange.Comment.FailsRollout);
            Assert.Equal(CommentKind.Caution, tableChange.Comment.Kind);
            Assert.Contains("Column order violation", tableChange.Comment.Text);
            Assert.Contains("Col2", tableChange.Comment.Text);
            Assert.Contains("NewCol", tableChange.Comment.Text);
        }

        [Fact]
        public void GenerateChanges_NewTable_NoValidationPerformed()
        {
            // Arrange
            var oldDb = new Database { Tables = new Dictionary<string, Table>() };
            var newDb = CreateDatabase(("Table1", new[] { ("NewCol", "bool"), ("Col1", "string") }));
            var settings = ValidationSettings.WithColumnOrderValidation();

            // Act
            var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

            // Assert
            var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
            Assert.NotNull(tableChange);
            Assert.Null(tableChange.Comment);
        }

        [Fact]
        public void GenerateChanges_MultipleTables_OnlyInvalidOnesGetComments()
        {
            // Arrange
            var oldDb = CreateDatabase(
                ("Table1", new[] { ("Col1", "string") }),
                ("Table2", new[] { ("Col1", "string") }),
                ("Table3", new[] { ("Col1", "string") }));

            var newDb = CreateDatabase(
                ("Table1", new[] { ("Col1", "string"), ("NewCol", "int") }),           // Valid
                ("Table2", new[] { ("NewCol", "int"), ("Col1", "string") }),           // Invalid
                ("Table3", new[] { ("Col1", "string"), ("AnotherCol", "bool") }));     // Valid

            // Act
            var settings = ValidationSettings.WithColumnOrderValidation();
            var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

            // Assert
            var table1Change = changes.FirstOrDefault(c => c.Entity == "Table1");
            var table2Change = changes.FirstOrDefault(c => c.Entity == "Table2");
            var table3Change = changes.FirstOrDefault(c => c.Entity == "Table3");

            Assert.Null(table1Change?.Comment);
            Assert.NotNull(table2Change?.Comment);
            Assert.True(table2Change.Comment.FailsRollout);
            Assert.Null(table3Change?.Comment);
        }

        [Fact]
        public void GenerateChanges_InvalidColumnOrder_ErrorMessageIncludesColumnNames()
        {
            // Arrange
            var oldDb = CreateDatabase(("EventsTable", new[] { ("EventId", "string"), ("Timestamp", "datetime") }));
            var newDb = CreateDatabase(("EventsTable", new[] { ("EventId", "string"), ("NewMetric", "int"), ("Timestamp", "datetime") }));

            // Act
            var settings = ValidationSettings.WithColumnOrderValidation();
            var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

            // Assert
            var tableChange = changes.FirstOrDefault(c => c.Entity == "EventsTable");
            Assert.NotNull(tableChange?.Comment);
            Assert.Contains("NewMetric", tableChange.Comment.Text);
            Assert.Contains("Timestamp", tableChange.Comment.Text);
            Assert.Contains("EventsTable", tableChange.Comment.Text);
        }

        [Fact]
        public void GenerateChanges_TableWithNoColumnsChanged_NoComment()
        {
            // Arrange
            var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
            var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));

            // Act
            var settings = ValidationSettings.WithColumnOrderValidation();
            var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

            // Assert
            var tableChanges = changes.Where(c => c.Entity == "Table1").ToList();
            // May have no changes at all if columns are identical
            Assert.All(tableChanges, change => Assert.Null(change.Comment));
        }

        [Fact]
        public void GenerateChanges_MultipleNewColumnsInMiddle_FailsValidation()
        {
            // Arrange
            var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int"), ("Col3", "bool") }));
            var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol1", "datetime"), ("NewCol2", "long"), ("Col2", "int"), ("Col3", "bool") }));

            // Act
            var settings = ValidationSettings.WithColumnOrderValidation();
            var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

            // Assert
            var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
            Assert.NotNull(tableChange?.Comment);
            Assert.True(tableChange.Comment.FailsRollout);
            Assert.Contains("Col2", tableChange.Comment.Text);
            Assert.Contains("Col3", tableChange.Comment.Text);
        }

        [Fact]
        public void GenerateChanges_ValidColumnOrderMultipleNewColumns_NoComment()
        {
            // Arrange
            var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
            var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int"), ("NewCol1", "bool"), ("NewCol2", "datetime"), ("NewCol3", "long") }));

            // Act
            var settings = ValidationSettings.WithColumnOrderValidation();
            var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

            // Assert
            var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
            if (tableChange != null)
            {
                Assert.Null(tableChange.Comment);
            }
        }

        private static Database CreateDatabase(params (string TableName, (string Name, string Type)[] Columns)[] tables)
        {
            var database = new Database
            {
                Tables = new Dictionary<string, Table>()
            };

            foreach (var table in tables)
            {
                var tableObj = new Table
                {
                    Columns = new Dictionary<string, string>()
                };

                foreach (var column in table.Columns)
                {
                    tableObj.Columns[column.Name] = column.Type;
                }

                database.Tables[table.TableName] = tableObj;
            }

            return database;
        }
    }
}
