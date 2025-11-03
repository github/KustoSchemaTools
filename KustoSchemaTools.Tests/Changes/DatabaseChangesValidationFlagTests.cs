using KustoSchemaTools.Changes;
using KustoSchemaTools.Configuration;
using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace KustoSchemaTools.Tests.Changes
{
    public class DatabaseChangesValidationFlagTests
    {
        private readonly Mock<ILogger> _loggerMock;

        public DatabaseChangesValidationFlagTests()
        {
            _loggerMock = new Mock<ILogger>();
        }

        [Fact]
        public void GenerateChanges_WithoutValidationSettings_DoesNotApplyValidation()
        {
            // Arrange
            var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
            var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int") }));

            // Act - No validation settings provided (null)
            var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object);

            // Assert
            var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
            Assert.NotNull(tableChange);
            Assert.Null(tableChange.Comment); // No validation comment should be attached
        }

        [Fact]
        public void GenerateChanges_WithValidationDisabled_DoesNotApplyValidation()
        {
            // Arrange
            var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
            var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int") }));
            var settings = new ValidationSettings { EnableColumnOrderValidation = false };

            // Act
            var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

            // Assert
            var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
            Assert.NotNull(tableChange);
            Assert.Null(tableChange.Comment); // No validation comment should be attached
        }

        [Fact]
        public void GenerateChanges_WithValidationEnabled_AppliesValidation()
        {
            // Arrange
            var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
            var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int") }));
            var settings = new ValidationSettings { EnableColumnOrderValidation = true };

            // Act
            var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

            // Assert
            var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
            Assert.NotNull(tableChange);
            Assert.NotNull(tableChange.Comment); // Validation comment should be attached
            Assert.True(tableChange.Comment.FailsRollout);
            Assert.Contains("Column order violation", tableChange.Comment.Text);
        }

        [Fact]
        public void GenerateChanges_WithValidationEnabledButValidColumnOrder_NoComment()
        {
            // Arrange
            var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
            var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int"), ("NewCol", "bool") }));
            var settings = new ValidationSettings { EnableColumnOrderValidation = true };

            // Act
            var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

            // Assert
            var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
            if (tableChange != null)
            {
                Assert.Null(tableChange.Comment); // No validation comment should be attached for valid order
            }
        }

        [Fact]
        public void GenerateChanges_WithValidationFromEnvironmentVariable_AppliesValidation()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", "true");
            var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
            var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int") }));
            var settings = ValidationSettings.FromEnvironment();

            try
            {
                // Act
                var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

                // Assert
                var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
                Assert.NotNull(tableChange);
                Assert.NotNull(tableChange.Comment); // Validation comment should be attached
                Assert.True(tableChange.Comment.FailsRollout);
                Assert.Contains("Column order violation", tableChange.Comment.Text);
            }
            finally
            {
                // Cleanup
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Fact]
        public void GenerateChanges_WithValidationFromEnvironmentVariableDisabled_DoesNotApplyValidation()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", "false");
            var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
            var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int") }));
            var settings = ValidationSettings.FromEnvironment();

            try
            {
                // Act
                var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, settings);

                // Assert
                var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
                Assert.NotNull(tableChange);
                Assert.Null(tableChange.Comment); // No validation comment should be attached
            }
            finally
            {
                // Cleanup
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Fact]
        public void GenerateChanges_WithConvenienceMethod_EnablesValidation()
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
            Assert.NotNull(tableChange.Comment); // Validation comment should be attached
            Assert.True(tableChange.Comment.FailsRollout);
            Assert.Contains("Column order violation", tableChange.Comment.Text);
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
