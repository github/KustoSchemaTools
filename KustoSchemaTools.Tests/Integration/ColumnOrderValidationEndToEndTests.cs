using KustoSchemaTools.Changes;
using KustoSchemaTools.Configuration;
using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace KustoSchemaTools.Tests.Integration
{
    /// <summary>
    /// End-to-end tests demonstrating the complete column order validation workflow
    /// for command-line usage scenarios.
    /// </summary>
    public class ColumnOrderValidationEndToEndTests
    {
        private readonly Mock<ILogger> _loggerMock;

        public ColumnOrderValidationEndToEndTests()
        {
            _loggerMock = new Mock<ILogger>();
        }

        [Fact]
        public void EndToEnd_EnvironmentVariableNotSet_ValidationDisabled_AllowsInvalidColumnOrder()
        {
            // Arrange - Ensure environment variable is not set
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            
            try
            {
                var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
                var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int") })); // Invalid order
                
                // Act - Use the same code path as command-line tools
                var validationSettings = ValidationSettings.FromEnvironment();
                var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, validationSettings);
                
                // Simulate checking if deployment should proceed
                var comments = changes.Select(itm => itm.Comment).Where(itm => itm != null).ToList();
                var isValid = changes.All(itm => itm.Scripts.All(itm => itm.IsValid != false)) && comments.All(itm => itm.FailsRollout == false);
                
                // Assert - Should be valid because validation is disabled by default
                Assert.False(validationSettings.EnableColumnOrderValidation);
                Assert.True(isValid, "When validation is disabled, invalid column order should not block deployment");
                
                // No validation comments should be present
                var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
                Assert.NotNull(tableChange);
                Assert.Null(tableChange.Comment);
            }
            finally
            {
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Fact]
        public void EndToEnd_EnvironmentVariableSetToTrue_ValidationEnabled_BlocksInvalidColumnOrder()
        {
            // Arrange - Set environment variable to enable validation
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", "true");
            
            try
            {
                var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
                var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int") })); // Invalid order
                
                // Act - Use the same code path as command-line tools
                var validationSettings = ValidationSettings.FromEnvironment();
                var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, validationSettings);
                
                // Simulate checking if deployment should proceed
                var comments = changes.Select(itm => itm.Comment).Where(itm => itm != null).ToList();
                var isValid = changes.All(itm => itm.Scripts.All(itm => itm.IsValid != false)) && comments.All(itm => itm.FailsRollout == false);
                
                // Assert - Should be invalid because validation is enabled and column order is wrong
                Assert.True(validationSettings.EnableColumnOrderValidation);
                Assert.False(isValid, "When validation is enabled, invalid column order should block deployment");
                
                // Should have validation failure comment
                var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
                Assert.NotNull(tableChange);
                Assert.NotNull(tableChange.Comment);
                Assert.True(tableChange.Comment.FailsRollout);
                Assert.Contains("Column order violation", tableChange.Comment.Text);
                Assert.Equal(CommentKind.Caution, tableChange.Comment.Kind);
            }
            finally
            {
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Fact]
        public void EndToEnd_EnvironmentVariableSetToTrue_ValidationEnabled_AllowsValidColumnOrder()
        {
            // Arrange - Set environment variable to enable validation
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", "true");
            
            try
            {
                var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
                var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int"), ("NewCol", "bool") })); // Valid order - new column at end
                
                // Act - Use the same code path as command-line tools
                var validationSettings = ValidationSettings.FromEnvironment();
                var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, validationSettings);
                
                // Simulate checking if deployment should proceed
                var comments = changes.Select(itm => itm.Comment).Where(itm => itm != null).ToList();
                var isValid = changes.All(itm => itm.Scripts.All(itm => itm.IsValid != false)) && comments.All(itm => itm.FailsRollout == false);
                
                // Assert - Should be valid because validation is enabled but column order is correct
                Assert.True(validationSettings.EnableColumnOrderValidation);
                Assert.True(isValid, "When validation is enabled, valid column order should allow deployment");
                
                // Should not have validation failure comment
                var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
                Assert.NotNull(tableChange);
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

        [Theory]
        [InlineData("true")]
        [InlineData("TRUE")]
        [InlineData("1")]
        public void EndToEnd_VariousEnvVariableFormats_EnableValidation(string envValue)
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", envValue);
            
            try
            {
                var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
                var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int") })); // Invalid order
                
                // Act
                var validationSettings = ValidationSettings.FromEnvironment();
                var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, validationSettings);
                
                var comments = changes.Select(itm => itm.Comment).Where(itm => itm != null).ToList();
                var isValid = changes.All(itm => itm.Scripts.All(itm => itm.IsValid != false)) && comments.All(itm => itm.FailsRollout == false);
                
                // Assert - All truthy values should enable validation and block deployment
                Assert.True(validationSettings.EnableColumnOrderValidation);
                Assert.False(isValid);
                
                var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
                Assert.NotNull(tableChange);
                Assert.NotNull(tableChange.Comment);
                Assert.True(tableChange.Comment.FailsRollout);
            }
            finally
            {
                Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", null);
            }
        }

        [Theory]
        [InlineData("false")]
        [InlineData("FALSE")]
        [InlineData("0")]
        [InlineData("invalid")]
        [InlineData("")]
        public void EndToEnd_VariousEnvVariableFormats_DisableValidation(string envValue)
        {
            // Arrange
            Environment.SetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION", envValue);
            
            try
            {
                var oldDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("Col2", "int") }));
                var newDb = CreateDatabase(("Table1", new[] { ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int") })); // Invalid order
                
                // Act
                var validationSettings = ValidationSettings.FromEnvironment();
                var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "TestDB", _loggerMock.Object, validationSettings);
                
                var comments = changes.Select(itm => itm.Comment).Where(itm => itm != null).ToList();
                var isValid = changes.All(itm => itm.Scripts.All(itm => itm.IsValid != false)) && comments.All(itm => itm.FailsRollout == false);
                
                // Assert - All falsy values should disable validation and allow deployment
                Assert.False(validationSettings.EnableColumnOrderValidation);
                Assert.True(isValid);
                
                var tableChange = changes.FirstOrDefault(c => c.Entity == "Table1");
                Assert.NotNull(tableChange);
                Assert.Null(tableChange.Comment);
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
