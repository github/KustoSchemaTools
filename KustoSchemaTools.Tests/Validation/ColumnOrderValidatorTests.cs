using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using KustoSchemaTools.Validation;

namespace KustoSchemaTools.Tests.Validation
{
    public class ColumnOrderValidatorTests
    {
        private readonly ColumnOrderValidator _validator;

        public ColumnOrderValidatorTests()
        {
            _validator = new ColumnOrderValidator();
        }

        [Fact]
        public void ValidateColumnOrder_IdenticalColumns_ReturnsSuccess()
        {
            // Arrange
            var baselineTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"));
            var proposedTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"));

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.True(result.IsValid);
            Assert.Null(result.ErrorMessage);
        }

        [Fact]
        public void ValidateColumnOrder_NewColumnsAppendedAtEnd_ReturnsSuccess()
        {
            // Arrange
            var baselineTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"));
            var proposedTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"), ("NewCol", "bool"));

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.True(result.IsValid);
        }

        [Fact]
        public void ValidateColumnOrder_ExistingColumnAfterNewColumn_ReturnsFailure()
        {
            // Arrange
            var baselineTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"));
            var proposedTable = CreateTable("Table1", ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int"));

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.False(result.IsValid);
            Assert.NotNull(result.ErrorMessage);
            Assert.Contains("Col2", result.ErrorMessage);
            Assert.Contains("NewCol", result.ErrorMessage);
            Assert.Contains("Column order violation", result.ErrorMessage);
            Assert.Equal(CommentKind.Caution, result.Severity);
        }

        [Fact]
        public void ValidateColumnOrder_NewTableWithNoBaseline_ReturnsSuccess()
        {
            // Arrange
            Table? baselineTable = null;
            var proposedTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"));

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.True(result.IsValid);
        }

        [Fact]
        public void ValidateColumnOrder_EmptyProposedColumns_ReturnsSuccess()
        {
            // Arrange
            var baselineTable = CreateTable("Table1", ("Col1", "string"));
            var proposedTable = new Table { Columns = new Dictionary<string, string>() };

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.True(result.IsValid);
        }

        [Fact]
        public void ValidateColumnOrder_NullProposedColumns_ReturnsSuccess()
        {
            // Arrange
            var baselineTable = CreateTable("Table1", ("Col1", "string"));
            var proposedTable = new Table { Columns = null! };

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.True(result.IsValid);
        }

        [Fact]
        public void ValidateColumnOrder_NullProposedTable_ReturnsSuccess()
        {
            // Arrange
            var baselineTable = CreateTable("Table1", ("Col1", "string"));
            Table? proposedTable = null;

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable!, "Table1");

            // Assert
            Assert.True(result.IsValid);
        }

        [Fact]
        public void ValidateColumnOrder_MultipleExistingColumnsInterspersed_ReturnsFailureWithAllMisplaced()
        {
            // Arrange
            var baselineTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"), ("Col3", "bool"));
            var proposedTable = CreateTable("Table1", ("Col1", "string"), ("NewCol1", "datetime"), ("Col2", "int"), ("NewCol2", "long"), ("Col3", "bool"));

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.False(result.IsValid);
            Assert.Contains("Col2", result.ErrorMessage);
            Assert.Contains("Col3", result.ErrorMessage);
            Assert.Contains("NewCol1", result.ErrorMessage);
            Assert.Contains("NewCol2", result.ErrorMessage);
        }

        [Fact]
        public void ValidateColumnOrder_AllColumnsAreNew_ReturnsSuccess()
        {
            // Arrange
            var baselineTable = new Table { Columns = new Dictionary<string, string>() };
            var proposedTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"));

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.True(result.IsValid);
        }

        [Fact]
        public void ValidateColumnOrder_MultipleNewColumnsAtEnd_ReturnsSuccess()
        {
            // Arrange
            var baselineTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"));
            var proposedTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"), ("NewCol1", "bool"), ("NewCol2", "datetime"), ("NewCol3", "long"));

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.True(result.IsValid);
        }

        [Fact]
        public void ValidateColumnOrder_SingleExistingColumnAtVeryEnd_ReturnsFailure()
        {
            // Arrange
            var baselineTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"), ("Col3", "bool"));
            var proposedTable = CreateTable("Table1", ("Col1", "string"), ("NewCol1", "datetime"), ("NewCol2", "long"), ("NewCol3", "decimal"), ("Col2", "int"), ("Col3", "bool"));

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.False(result.IsValid);
            Assert.Contains("Col2", result.ErrorMessage);
            Assert.Contains("Col3", result.ErrorMessage);
        }

        [Fact]
        public void ValidateColumnOrder_NoNewColumnsOnlyExisting_ReturnsSuccess()
        {
            // Arrange
            var baselineTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"));
            var proposedTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"));

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.True(result.IsValid);
        }

        [Fact]
        public void ValidateColumnOrder_ErrorMessageContainsTableName()
        {
            // Arrange
            var baselineTable = CreateTable("EventsTable", ("EventId", "string"), ("Timestamp", "datetime"));
            var proposedTable = CreateTable("EventsTable", ("EventId", "string"), ("NewMetric", "int"), ("Timestamp", "datetime"));

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "EventsTable");

            // Assert
            Assert.False(result.IsValid);
            Assert.Contains("EventsTable", result.ErrorMessage);
        }

        [Fact]
        public void ValidateColumnOrder_ErrorMessageExplainsKustoBehavior()
        {
            // Arrange
            var baselineTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"));
            var proposedTable = CreateTable("Table1", ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int"));

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.Contains("Kusto preserves column ordinal positions", result.ErrorMessage);
            Assert.Contains("ALTER TABLE operations", result.ErrorMessage);
            Assert.Contains("update policy validation failures", result.ErrorMessage);
        }

        [Fact]
        public void ValidateColumnOrder_ErrorMessageProvidesActionGuidance()
        {
            // Arrange
            var baselineTable = CreateTable("Table1", ("Col1", "string"), ("Col2", "int"));
            var proposedTable = CreateTable("Table1", ("Col1", "string"), ("NewCol", "bool"), ("Col2", "int"));

            // Act
            var result = _validator.ValidateColumnOrder(baselineTable, proposedTable, "Table1");

            // Assert
            Assert.Contains("Action required", result.ErrorMessage);
            Assert.Contains("Move all new columns to the end", result.ErrorMessage);
        }

        private static Table CreateTable(string name, params (string Name, string Type)[] columns)
        {
            var table = new Table
            {
                Columns = new Dictionary<string, string>()
            };

            foreach (var column in columns)
            {
                table.Columns[column.Name] = column.Type;
            }

            return table;
        }
    }
}
