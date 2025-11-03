using KustoSchemaTools.Changes;
using KustoSchemaTools.Configuration;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser.KustoWriter;
using KustoSchemaTools.Parser;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace KustoSchemaTools.Tests.Integration
{
    public class ColumnOrderValidationIntegrationTests
    {
        private readonly Mock<ILogger> _loggerMock;

        public ColumnOrderValidationIntegrationTests()
        {
            _loggerMock = new Mock<ILogger>();
        }

        [Fact]
        public void ValidationSettings_FromEnvironment_WhenNotSet_ReturnsDisabledSettings()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);

            try
            {
                // Act
                var settings = ValidationSettings.FromEnvironment();

                // Assert
                Assert.False(settings.EnableColumnOrderValidation);
            }
            finally
            {
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Fact]
        public void ValidationSettings_FromEnvironment_WhenSetToTrue_ReturnsEnabledSettings()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", "true");

            try
            {
                // Act
                var settings = ValidationSettings.FromEnvironment();

                // Assert
                Assert.True(settings.EnableColumnOrderValidation);
            }
            finally
            {
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Fact]
        public void DatabaseChanges_WithEnvironmentValidationEnabled_BlocksInvalidColumnOrder()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", "true");
            
            try
            {
                var sourceDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int") })); // Invalid order
                var targetDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") })); // Original state
                
                var validationSettings = ValidationSettings.FromEnvironment();

                // Act
                var changes = DatabaseChanges.GenerateChanges(targetDb, sourceDb, "TestDB", _loggerMock.Object, validationSettings);
                
                // Assert
                var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
                Assert.NotNull(tableChange);
                Assert.NotNull(tableChange.Comment);
                Assert.True(tableChange.Comment.FailsRollout);
                Assert.Contains("Column order violation", tableChange.Comment.Text);
            }
            finally
            {
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Fact]
        public void DatabaseChanges_WithEnvironmentValidationDisabled_AllowsInvalidColumnOrder()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", "false");
            
            try
            {
                var sourceDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int") })); // Invalid order
                var targetDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") })); // Original state
                
                var validationSettings = ValidationSettings.FromEnvironment();

                // Act
                var changes = DatabaseChanges.GenerateChanges(targetDb, sourceDb, "TestDB", _loggerMock.Object, validationSettings);
                
                // Assert - No validation comment should be attached when validation is disabled
                var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
                Assert.NotNull(tableChange);
                Assert.Null(tableChange.Comment); // Should not have validation comment when disabled
            }
            finally
            {
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Fact]
        public void DatabaseChanges_WithEnvironmentValidationEnabled_AllowsValidColumnOrder()
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", "true");
            
            try
            {
                var sourceDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int"), ("NewCol", "bool") })); // Valid order - new column at end
                var targetDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") })); // Original state
                
                var validationSettings = ValidationSettings.FromEnvironment();

                // Act
                var changes = DatabaseChanges.GenerateChanges(targetDb, sourceDb, "TestDB", _loggerMock.Object, validationSettings);
                
                // Assert - Should not have validation failure comment for valid column order
                var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
                Assert.NotNull(tableChange);
                // Should not have a validation failure comment (could have other comments, but not validation failure)
                if (tableChange.Comment != null)
                {
                    Assert.False(tableChange.Comment.FailsRollout);
                    Assert.DoesNotContain("Column order violation", tableChange.Comment.Text);
                }
            }
            finally
            {
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        private static Database CreateDatabase(params (string TableName, (string Name, string Type)[] Columns)[] tables)
        {
            var database = new Database
            {
                Name = "TestDB",
                Tables = new Dictionary<string, Table>(),
                Admins = new List<AADObject>(),
                Users = new List<AADObject>(),
                Viewers = new List<AADObject>(),
                Monitors = new List<AADObject>(),
                Ingestors = new List<AADObject>(),
                UnrestrictedViewers = new List<AADObject>(),
                Functions = new Dictionary<string, Function>(),
                MaterializedViews = new Dictionary<string, MaterializedView>(),
                ContinuousExports = new Dictionary<string, ContinuousExport>(),
                ExternalTables = new Dictionary<string, ExternalTable>(),
                EntityGroups = new Dictionary<string, List<Entity>>(),
                Followers = new Dictionary<string, FollowerDatabase>(),
                Deletions = new Deletions(),
                Scripts = new List<DatabaseScript>()
            };

            foreach (var (tableName, columns) in tables)
            {
                var table = new Table
                {
                    Columns = new Dictionary<string, string>(),
                    Folder = "",
                    DocString = "",
                    Scripts = new List<DatabaseScript>()
                };

                foreach (var (name, type) in columns)
                {
                    table.Columns[name] = type;
                }

                database.Tables[tableName] = table;
            }

            return database;
        }
    }
}
