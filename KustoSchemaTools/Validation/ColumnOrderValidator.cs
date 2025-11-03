using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Validation
{
    /// <summary>
    /// Validates that new columns in table definitions are appended at the end
    /// to prevent update policy failures from column ordinal position changes.
    /// </summary>
    public class ColumnOrderValidator
    {
        /// <summary>
        /// Validates that new columns are only appended to the end of the column list.
        /// </summary>
        /// <param name="baselineTable">The existing table definition, or null if this is a new table.</param>
        /// <param name="proposedTable">The proposed table definition to validate.</param>
        /// <param name="tableName">The name of the table being validated.</param>
        /// <returns>A ValidationResult indicating success or failure with detailed error information.</returns>
        public ValidationResult ValidateColumnOrder(Table? baselineTable, Table proposedTable, string tableName)
        {
            if (proposedTable == null)
            {
                return ValidationResult.Success();
            }

            if (proposedTable.Columns == null || proposedTable.Columns.Count == 0)
            {
                return ValidationResult.Success();
            }

            // New tables can have any column order
            if (baselineTable == null || baselineTable.Columns == null || baselineTable.Columns.Count == 0)
            {
                return ValidationResult.Success();
            }

            return ValidateColumnOrder(
                baselineTable.Columns,
                proposedTable.Columns,
                tableName);
        }

        /// <summary>
        /// Validates column order when both baseline and proposed columns exist.
        /// </summary>
        private ValidationResult ValidateColumnOrder(
            Dictionary<string, string> baselineColumns,
            Dictionary<string, string> proposedColumns,
            string tableName)
        {
            var baselineColumnNames = baselineColumns.Keys.ToHashSet();
            var proposedColumnList = proposedColumns.Keys.ToList();

            // Find all new columns
            var newColumns = proposedColumnList
                .Where(name => !baselineColumnNames.Contains(name))
                .ToList();

            if (newColumns.Count == 0)
            {
                return ValidationResult.Success();
            }

            // Find the position of the first new column
            int firstNewColumnIndex = proposedColumnList.FindIndex(name => newColumns.Contains(name));

            // Check if any existing columns appear after the first new column
            var misplacedColumns = new List<string>();
            for (int i = firstNewColumnIndex + 1; i < proposedColumnList.Count; i++)
            {
                string columnName = proposedColumnList[i];
                if (baselineColumnNames.Contains(columnName))
                {
                    misplacedColumns.Add(columnName);
                }
            }

            if (misplacedColumns.Count > 0)
            {
                string newColumnsText = string.Join(", ", newColumns);
                string misplacedColumnsText = string.Join(", ", misplacedColumns);

                string errorMessage = $"Column order violation detected in table '{tableName}'. " +
                    $"New columns must be appended to the end of the table definition. " +
                    $"Found existing columns ({misplacedColumnsText}) positioned after new columns ({newColumnsText}). " +
                    $"Kusto preserves column ordinal positions after ALTER TABLE operations, which will cause " +
                    $"update policy validation failures if columns are inserted in the middle. " +
                    $"Action required: Move all new columns to the end of the columns list.";

                return ValidationResult.Failure(errorMessage, CommentKind.Caution);
            }

            return ValidationResult.Success();
        }
    }
}
