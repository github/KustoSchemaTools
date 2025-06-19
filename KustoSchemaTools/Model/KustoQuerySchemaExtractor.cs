using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;

namespace KustoSchemaTools.Model
{
    /// <summary>
    /// Example of using the Kusto Language Service to extract schema information from KQL queries.
    /// This demonstrates how to use the official Kusto parser instead of regex-based parsing.
    /// </summary>
    public static class KustoQuerySchemaExtractor
    {
        /// <summary>
        /// Extracts the output schema from a KQL query using the official Kusto parser.
        /// </summary>
        /// <param name="query">The KQL query to analyze</param>
        /// <param name="sourceTableSchema">Schema of the source table (optional)</param>
        /// <returns>Dictionary of column names and their inferred types</returns>
        /// <example>
        /// var schema = KustoQuerySchemaExtractor.ExtractOutputSchema(
        ///     "SourceTable | project EventId, ProcessedTime = now()",
        ///     new Dictionary&lt;string, string&gt; { {"EventId", "string"}, {"Timestamp", "datetime"} }
        /// );
        /// // Result: {"EventId": "string", "ProcessedTime": "datetime"}
        /// </example>
        public static Dictionary<string, string> ExtractOutputSchema(string query, Dictionary<string, string>? sourceTableSchema = null)
        {
            var outputSchema = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            try
            {
                // Create a database with the source table if schema is provided
                var database = CreateDatabaseWithSourceTable(sourceTableSchema);
                
                // Parse the query with database context
                var globalState = GlobalState.Default.WithDatabase(database);
                var parsedQuery = KustoCode.ParseAndAnalyze(query, globalState);
                
                // Check for syntax errors
                var diagnostics = parsedQuery.GetDiagnostics();
                var errors = diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
                if (errors.Any())
                {
                    throw new InvalidOperationException($"Query has syntax errors: {string.Join(", ", errors.Select(e => e.Message))}");
                }

                // Get the result schema from the query
                if (parsedQuery.ResultType is TableSymbol resultTable)
                {
                    foreach (var column in resultTable.Columns)
                    {
                        outputSchema[column.Name] = ConvertKustoTypeToString(column.Type);
                    }
                }

                return outputSchema;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to extract schema from query: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Validates a KQL query for syntax and semantic correctness using the official parser.
        /// </summary>
        /// <param name="query">The KQL query to validate</param>
        /// <param name="sourceTableSchema">Schema of the source table</param>
        /// <param name="sourceTableName">Name of the source table</param>
        /// <returns>Validation result with errors, warnings, and extracted information</returns>
        /// <example>
        /// var result = KustoQuerySchemaExtractor.ValidateQuery(
        ///     "SourceTable | where Timestamp > ago(1h) | project EventId",
        ///     new Dictionary&lt;string, string&gt; { {"EventId", "string"}, {"Timestamp", "datetime"} }
        /// );
        /// 
        /// if (result.IsValid) {
        ///     Console.WriteLine($"Output schema: {string.Join(", ", result.OutputSchema)}");
        ///     Console.WriteLine($"Referenced columns: {string.Join(", ", result.ReferencedColumns)}");
        /// }
        /// </example>
        public static KustoQueryValidationResult ValidateQuery(string query, Dictionary<string, string>? sourceTableSchema = null, string sourceTableName = "SourceTable")
        {
            var result = new KustoQueryValidationResult();

            try
            {
                // Create a database with the source table
                var database = CreateDatabaseWithSourceTable(sourceTableSchema, sourceTableName);
                
                // Parse and analyze the query
                var globalState = GlobalState.Default.WithDatabase(database);
                var parsedQuery = KustoCode.ParseAndAnalyze(query, globalState);
                
                // Collect syntax and semantic errors
                var diagnostics = parsedQuery.GetDiagnostics();
                foreach (var diagnostic in diagnostics)
                {
                    if (diagnostic.Severity == DiagnosticSeverity.Error)
                    {
                        result.Errors.Add($"Error at position {diagnostic.Start}: {diagnostic.Message}");
                    }
                    else if (diagnostic.Severity == DiagnosticSeverity.Warning)
                    {
                        result.Warnings.Add($"Warning at position {diagnostic.Start}: {diagnostic.Message}");
                    }
                }

                // If no errors, extract additional information
                if (!result.HasErrors)
                {
                    // Extract output schema
                    if (parsedQuery.ResultType is TableSymbol resultTable)
                    {
                        foreach (var column in resultTable.Columns)
                        {
                            result.OutputSchema[column.Name] = ConvertKustoTypeToString(column.Type);
                        }
                    }

                    // Extract column references by walking the syntax tree
                    var sourceColumns = sourceTableSchema?.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase) 
                                       ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    ExtractColumnReferencesFromSyntax(parsedQuery.Syntax, result.ReferencedColumns, sourceColumns);
                }

                return result;
            }
            catch (Exception ex)
            {
                result.Errors.Add($"Query validation failed: {ex.Message}");
                return result;
            }
        }

        /// <summary>
        /// Creates a database symbol with a source table for query analysis.
        /// </summary>
        private static DatabaseSymbol CreateDatabaseWithSourceTable(Dictionary<string, string>? sourceTableSchema, string tableName = "SourceTable")
        {
            var tables = new List<TableSymbol>();

            if (sourceTableSchema != null && sourceTableSchema.Any())
            {
                var columns = sourceTableSchema.Select(kvp => 
                    new ColumnSymbol(kvp.Key, ConvertStringTypeToKustoType(kvp.Value))
                ).ToArray();

                var table = new TableSymbol(tableName, columns);
                tables.Add(table);
            }
            else
            {
                // Create a default table with common columns if no schema provided
                var defaultColumns = new[]
                {
                    new ColumnSymbol("EventId", ScalarTypes.String),
                    new ColumnSymbol("Timestamp", ScalarTypes.DateTime),
                    new ColumnSymbol("Data", ScalarTypes.Dynamic)
                };
                
                var table = new TableSymbol(tableName, defaultColumns);
                tables.Add(table);
            }

            return new DatabaseSymbol("TestDatabase", tables);
        }

        /// <summary>
        /// Recursively walks the syntax tree to find column references.
        /// Only includes references to columns from the source table.
        /// </summary>
        private static void ExtractColumnReferencesFromSyntax(SyntaxNode node, HashSet<string> referencedColumns, HashSet<string> sourceColumns)
        {
            // Look for name references that resolve to columns
            if (node is NameReference nameRef && nameRef.ReferencedSymbol is ColumnSymbol)
            {
                var columnName = nameRef.Name.SimpleName;
                
                // Only include if it's a column from the original source table
                if (sourceColumns.Contains(columnName))
                {
                    referencedColumns.Add(columnName);
                }
            }

            // Recursively process child nodes
            foreach (var child in node.GetDescendants<SyntaxNode>())
            {
                ExtractColumnReferencesFromSyntax(child, referencedColumns, sourceColumns);
            }
        }

        /// <summary>
        /// Converts a Kusto TypeSymbol to a string representation.
        /// </summary>
        private static string ConvertKustoTypeToString(TypeSymbol type)
        {
            return type switch
            {
                _ when type == ScalarTypes.String => "string",
                _ when type == ScalarTypes.Int => "int",
                _ when type == ScalarTypes.Long => "long",
                _ when type == ScalarTypes.Real => "real",
                _ when type == ScalarTypes.Bool => "bool",
                _ when type == ScalarTypes.DateTime => "datetime",
                _ when type == ScalarTypes.TimeSpan => "timespan",
                _ when type == ScalarTypes.Dynamic => "dynamic",
                _ when type == ScalarTypes.Guid => "guid",
                _ when type == ScalarTypes.Decimal => "decimal",
                _ => "dynamic" // Default to dynamic for unknown types
            };
        }

        /// <summary>
        /// Converts a string type representation to a Kusto TypeSymbol.
        /// </summary>
        private static TypeSymbol ConvertStringTypeToKustoType(string typeString)
        {
            return typeString.ToLowerInvariant() switch
            {
                "string" => ScalarTypes.String,
                "int" => ScalarTypes.Int,
                "long" => ScalarTypes.Long,
                "real" => ScalarTypes.Real,
                "bool" or "boolean" => ScalarTypes.Bool,
                "datetime" => ScalarTypes.DateTime,
                "timespan" => ScalarTypes.TimeSpan,
                "guid" => ScalarTypes.Guid,
                "decimal" => ScalarTypes.Decimal,
                "dynamic" => ScalarTypes.Dynamic,
                _ => ScalarTypes.Dynamic // Default to dynamic for unknown types
            };
        }

        /// <summary>
        /// Extracts column references from a KQL query using the official Kusto parser.
        /// </summary>
        /// <param name="query">The KQL query to analyze</param>
        /// <param name="sourceTableName">Name of the source table</param>
        /// <param name="sourceTableSchema">Schema of the source table</param>
        /// <returns>Set of column names referenced in the query</returns>
        /// <example>
        /// var references = KustoQuerySchemaExtractor.ExtractColumnReferences(
        ///     "SourceTable | where EventId != '' | project EventId, Timestamp",
        ///     "SourceTable",
        ///     new Dictionary&lt;string, string&gt; { {"EventId", "string"}, {"Timestamp", "datetime"} }
        /// );
        /// // Result: {"EventId", "Timestamp"}
        /// </example>
        public static HashSet<string> ExtractColumnReferences(string query, string sourceTableName, Dictionary<string, string>? sourceTableSchema = null)
        {
            var referencedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            try
            {
                // Create a database with the source table
                var database = CreateDatabaseWithSourceTable(sourceTableSchema, sourceTableName);
                
                // Parse and analyze the query
                var globalState = GlobalState.Default.WithDatabase(database);
                var parsedQuery = KustoCode.ParseAndAnalyze(query, globalState);
                
                // Get the source table columns for filtering
                var sourceColumns = sourceTableSchema?.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase) 
                                   ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                // Extract column references by walking the syntax tree
                ExtractColumnReferencesFromSyntax(parsedQuery.Syntax, referencedColumns, sourceColumns);

                return referencedColumns;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to extract column references from query: {ex.Message}", ex);
            }
        }
    }

    /// <summary>
    /// Result of Kusto query validation using the official parser.
    /// Contains errors, warnings, output schema, and referenced columns.
    /// </summary>
    public class KustoQueryValidationResult
    {
        public List<string> Errors { get; } = new List<string>();
        public List<string> Warnings { get; } = new List<string>();
        public Dictionary<string, string> OutputSchema { get; set; } = new Dictionary<string, string>();
        public HashSet<string> ReferencedColumns { get; set; } = new HashSet<string>();

        public bool IsValid => !HasErrors;
        public bool HasErrors => Errors.Any();
        public bool HasWarnings => Warnings.Any();

        public override string ToString()
        {
            var messages = new List<string>();
            
            if (HasErrors)
            {
                messages.Add($"Errors: {string.Join(", ", Errors)}");
            }
            
            if (HasWarnings)
            {
                messages.Add($"Warnings: {string.Join(", ", Warnings)}");
            }

            if (!HasErrors)
            {
                messages.Add($"Output columns: {string.Join(", ", OutputSchema.Keys)}");
                messages.Add($"Referenced columns: {string.Join(", ", ReferencedColumns)}");
            }

            return messages.Any() ? string.Join("; ", messages) : "Valid";
        }
    }
}
