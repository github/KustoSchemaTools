using System.Text.RegularExpressions;

namespace KustoSchemaTools.Model
{
    /// <summary>
    /// Configuration options for update policy validation behavior.
    /// </summary>
    public class UpdatePolicyValidationConfig
    {
        /// <summary>
        /// Gets or sets whether to enforce strict type compatibility.
        /// When false (default), allows implicit conversions between numeric types (int, long, real, decimal).
        /// When true, requires exact type matches except for dynamic types.
        /// </summary>
        public bool EnforceStrictTypeCompatibility { get; set; } = false;

        /// <summary>
        /// Default configuration instance with permissive numeric type compatibility.
        /// </summary>
        public static UpdatePolicyValidationConfig Default => new UpdatePolicyValidationConfig();

        /// <summary>
        /// Strict configuration instance that requires exact type matches.
        /// </summary>
        public static UpdatePolicyValidationConfig Strict => new UpdatePolicyValidationConfig 
        { 
            EnforceStrictTypeCompatibility = true 
        };
    }

    /// <summary>
    /// Provides validation functionality for UpdatePolicy objects before they are applied to tables.
    /// </summary>
    public static class UpdatePolicyValidator
    {
        /// <summary>
        /// Validates an update policy against a target table schema.
        /// </summary>
        /// <param name="updatePolicy">The update policy to validate</param>
        /// <param name="targetTable">The target table the policy will be applied to</param>
        /// <param name="sourceTable">The source table referenced in the policy (optional, for schema comparison)</param>
        /// <param name="database">The database context containing all tables</param>
        /// <returns>A validation result indicating whether the policy is valid</returns>
        public static UpdatePolicyValidationResult ValidatePolicy(
            UpdatePolicy updatePolicy, 
            Table targetTable, 
            Table? sourceTable, 
            Database database)
        {
            return ValidatePolicy(updatePolicy, targetTable, sourceTable, database, UpdatePolicyValidationConfig.Default);
        }

        /// <summary>
        /// Validates an update policy against a target table schema with custom configuration.
        /// </summary>
        /// <param name="updatePolicy">The update policy to validate</param>
        /// <param name="targetTable">The target table the policy will be applied to</param>
        /// <param name="sourceTable">The source table referenced in the policy (optional, for schema comparison)</param>
        /// <param name="database">The database context containing all tables</param>
        /// <param name="config">Configuration options for validation behavior</param>
        /// <returns>A validation result indicating whether the policy is valid</returns>
        public static UpdatePolicyValidationResult ValidatePolicy(
            UpdatePolicy updatePolicy, 
            Table targetTable, 
            Table? sourceTable, 
            Database database,
            UpdatePolicyValidationConfig config)
        {
            var result = new UpdatePolicyValidationResult();

            if (updatePolicy == null)
            {
                result.AddError("UpdatePolicy cannot be null");
                return result;
            }

            if (targetTable == null)
            {
                result.AddError("Target table cannot be null");
                return result;
            }

            if (config == null)
            {
                config = UpdatePolicyValidationConfig.Default;
            }

            // Validate basic properties
            ValidateBasicProperties(updatePolicy, result);

            // Validate source table exists
            ValidateSourceTable(updatePolicy, database, result);

            // Validate schema compatibility
            if (result.IsValid)
            {
                ValidateSchemaCompatibility(updatePolicy, targetTable, sourceTable, result, config);
            }

            return result;
        }

        /// <summary>
        /// Validates basic properties of the update policy.
        /// </summary>
        private static void ValidateBasicProperties(UpdatePolicy updatePolicy, UpdatePolicyValidationResult result)
        {
            if (string.IsNullOrWhiteSpace(updatePolicy.Source))
            {
                result.AddError("UpdatePolicy.Source cannot be null or empty");
            }

            if (string.IsNullOrWhiteSpace(updatePolicy.Query))
            {
                result.AddError("UpdatePolicy.Query cannot be null or empty");
            }

            // Validate managed identity if specified
            if (!string.IsNullOrWhiteSpace(updatePolicy.ManagedIdentity))
            {
                if (!IsValidManagedIdentity(updatePolicy.ManagedIdentity))
                {
                    result.AddWarning($"Managed identity '{updatePolicy.ManagedIdentity}' format may be invalid");
                }
            }
        }

        /// <summary>
        /// Validates that the source table exists in the database.
        /// </summary>
        private static void ValidateSourceTable(UpdatePolicy updatePolicy, Database database, UpdatePolicyValidationResult result)
        {
            if (database?.Tables == null)
            {
                result.AddWarning("Database or Tables collection is null, cannot validate source table existence");
                return;
            }

            if (!database.Tables.ContainsKey(updatePolicy.Source))
            {
                result.AddError($"Source table '{updatePolicy.Source}' does not exist in the database");
            }
        }

        /// <summary>
        /// Validates schema compatibility between source and target tables.
        /// Now uses the official Kusto parser for more accurate analysis.
        /// </summary>
        private static void ValidateSchemaCompatibility(
            UpdatePolicy updatePolicy, 
            Table targetTable, 
            Table sourceTable, 
            UpdatePolicyValidationResult result,
            UpdatePolicyValidationConfig config)
        {
            if (targetTable.Columns == null || sourceTable.Columns == null)
            {
                result.AddWarning("Cannot validate schema compatibility: table columns are not defined");
                return;
            }

            try
            {
                // Use the Kusto parser for accurate query validation
                var queryValidation = KustoQuerySchemaExtractor.ValidateQuery(
                    updatePolicy.Query, 
                    sourceTable.Columns, 
                    updatePolicy.Source);

                // Add any query syntax/semantic errors
                foreach (var error in queryValidation.Errors)
                {
                    result.AddError($"Query validation error: {error}");
                }

                foreach (var warning in queryValidation.Warnings)
                {
                    result.AddWarning($"Query validation warning: {warning}");
                }

                // If query is valid, check schema compatibility
                if (queryValidation.IsValid)
                {
                    ValidateOutputSchemaCompatibility(queryValidation.OutputSchema, targetTable, result, config);
                    ValidateColumnReferences(queryValidation.ReferencedColumns, sourceTable, updatePolicy.Source, result);
                }
            }
            catch (Exception ex)
            {
                // Fallback to regex-based validation if parser fails
                result.AddWarning($"Parser-based validation failed ({ex.Message}), falling back to regex-based validation");
                ValidateSchemaCompatibilityWithRegex(updatePolicy, targetTable, sourceTable, result, config);
            }
        }

        /// <summary>
        /// Validates that the query output schema matches the target table schema.
        /// </summary>
        private static void ValidateOutputSchemaCompatibility(
            Dictionary<string, string> outputSchema,
            Table targetTable,
            UpdatePolicyValidationResult result,
            UpdatePolicyValidationConfig config)
        {
            // Check if query produces columns that exist in target table
            foreach (var targetColumn in targetTable.Columns!)
            {
                if (outputSchema.TryGetValue(targetColumn.Key, out var queryColumnType))
                {
                    if (!AreTypesCompatible(queryColumnType, targetColumn.Value, config))
                    {
                        result.AddError($"Column '{targetColumn.Key}' type mismatch: query produces '{queryColumnType}' but target table expects '{targetColumn.Value}'");
                    }
                }
                else
                {
                    result.AddWarning($"Target table column '{targetColumn.Key}' is not produced by the query");
                }
            }

            // Check for columns in query that don't exist in target
            foreach (var queryColumn in outputSchema)
            {
                if (!targetTable.Columns.ContainsKey(queryColumn.Key))
                {
                    result.AddWarning($"Query produces column '{queryColumn.Key}' which does not exist in target table");
                }
            }
        }

        /// <summary>
        /// Validates that all column references in the query exist in the source table.
        /// </summary>
        private static void ValidateColumnReferences(
            HashSet<string> referencedColumns,
            Table sourceTable,
            string sourceTableName,
            UpdatePolicyValidationResult result)
        {
            if (sourceTable.Columns == null) return;

            foreach (var referencedColumn in referencedColumns)
            {
                if (!sourceTable.Columns.ContainsKey(referencedColumn))
                {
                    result.AddError($"Query references column '{referencedColumn}' which does not exist in source table '{sourceTableName}'");
                }
            }
        }

        /// <summary>
        /// Fallback regex-based schema compatibility validation.
        /// Used when the Kusto parser fails for any reason.
        /// </summary>
        private static void ValidateSchemaCompatibilityWithRegex(
            UpdatePolicy updatePolicy, 
            Table targetTable, 
            Table sourceTable, 
            UpdatePolicyValidationResult result,
            UpdatePolicyValidationConfig config)
        {
            // Extract column references from the query using regex (original implementation)
            var queryColumns = ExtractColumnReferencesFromQuery(updatePolicy.Query);
            
            // Check if query produces columns that exist in target table
            foreach (var targetColumn in targetTable.Columns!)
            {
                // If the query explicitly projects this column, validate its type compatibility
                if (queryColumns.ContainsKey(targetColumn.Key))
                {
                    var queryColumnType = queryColumns[targetColumn.Key];
                    if (!AreTypesCompatible(queryColumnType, targetColumn.Value, config))
                    {
                        result.AddError($"Column '{targetColumn.Key}' type mismatch: query produces '{queryColumnType}' but target table expects '{targetColumn.Value}'");
                    }
                }
            }

            // Check for columns in query that don't exist in target
            foreach (var queryColumn in queryColumns)
            {
                if (!targetTable.Columns.ContainsKey(queryColumn.Key))
                {
                    result.AddWarning($"Query produces column '{queryColumn.Key}' which does not exist in target table");
                }
            }
        }

        /// <summary>
        /// Validates that columns referenced in the query exist in the source table.
        /// Now uses the official Kusto parser for more accurate analysis.
        /// </summary>
        private static void ValidateQueryColumns(
            UpdatePolicy updatePolicy, 
            Table targetTable, 
            Table? sourceTable, 
            UpdatePolicyValidationResult result)
        {
            if (sourceTable?.Columns == null)
            {
                result.AddWarning("Cannot validate query column references: source table columns are not defined");
                return;
            }

            try
            {
                // Use the Kusto parser for accurate column reference extraction
                var referencedColumns = KustoQuerySchemaExtractor.ExtractColumnReferences(
                    updatePolicy.Query, 
                    updatePolicy.Source, 
                    sourceTable.Columns);

                foreach (var columnRef in referencedColumns)
                {
                    if (!sourceTable.Columns.ContainsKey(columnRef))
                    {
                        result.AddError($"Query references column '{columnRef}' which does not exist in source table '{updatePolicy.Source}'");
                    }
                }
            }
            catch (Exception ex)
            {
                // Fallback to regex-based validation if parser fails
                result.AddWarning($"Parser-based column reference extraction failed ({ex.Message}), falling back to regex-based validation");
                ValidateQueryColumnsWithRegex(updatePolicy, sourceTable, result);
            }
        }

        /// <summary>
        /// Fallback regex-based column reference validation.
        /// Used when the Kusto parser fails for any reason.
        /// </summary>
        private static void ValidateQueryColumnsWithRegex(
            UpdatePolicy updatePolicy, 
            Table sourceTable, 
            UpdatePolicyValidationResult result)
        {
            // Extract source column references from the query using regex (original implementation)
            var sourceColumnReferences = ExtractSourceColumnReferences(updatePolicy.Query, updatePolicy.Source);

            foreach (var columnRef in sourceColumnReferences)
            {
                if (!sourceTable.Columns!.ContainsKey(columnRef))
                {
                    result.AddError($"Query references column '{columnRef}' which does not exist in source table '{updatePolicy.Source}'");
                }
            }
        }

        /// <summary>
        /// Extracts column references and their types from a KQL query (simplified implementation).
        /// </summary>
        private static Dictionary<string, string> ExtractColumnReferencesFromQuery(string query)
        {
            var columns = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            
            // This is a simplified implementation that looks for basic patterns
            // In a production environment, you'd want to use the Kusto query parser
            
            // Look for "| project" statements
            var projectMatches = Regex.Matches(query, @"\|\s*project\s+([^|]+)", RegexOptions.IgnoreCase);
            foreach (Match match in projectMatches)
            {
                var projectClause = match.Groups[1].Value.Trim();
                var columnDefs = projectClause.Split(',');
                
                foreach (var columnDef in columnDefs)
                {
                    var parts = columnDef.Split('=');
                    if (parts.Length >= 2)
                    {
                        var columnName = parts[0].Trim();
                        // Try to infer type from expression (very basic)
                        var expression = parts[1].Trim();
                        var inferredType = InferTypeFromExpression(expression);
                        columns[columnName] = inferredType;
                    }
                    else
                    {
                        // Simple column reference
                        var columnName = columnDef.Trim();
                        columns[columnName] = "dynamic"; // Default to dynamic if we can't determine type
                    }
                }
            }

            // If no explicit project, assume all source columns are passed through
            if (columns.Count == 0)
            {
                // Look for extend statements to find new columns
                var extendMatches = Regex.Matches(query, @"\|\s*extend\s+([^|]+)", RegexOptions.IgnoreCase);
                foreach (Match match in extendMatches)
                {
                    var extendClause = match.Groups[1].Value.Trim();
                    var columnDefs = extendClause.Split(',');
                    
                    foreach (var columnDef in columnDefs)
                    {
                        var parts = columnDef.Split('=');
                        if (parts.Length >= 2)
                        {
                            var columnName = parts[0].Trim();
                            var expression = parts[1].Trim();
                            var inferredType = InferTypeFromExpression(expression);
                            columns[columnName] = inferredType;
                        }
                    }
                }
            }

            return columns;
        }

        /// <summary>
        /// Extracts source column references from a KQL query.
        /// </summary>
        private static HashSet<string> ExtractSourceColumnReferences(string query, string sourceTableName)
        {
            var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            
            // Look for column references in the query
            // This is a simplified implementation - in production you'd use the Kusto query parser
            
            // Remove the source table reference from the beginning
            var queryWithoutSource = Regex.Replace(query, $@"^{Regex.Escape(sourceTableName)}\s*\|?\s*", "", RegexOptions.IgnoreCase);
            
            // Parse the query to find actual column references, excluding column assignments
            var parts = queryWithoutSource.Split('|');
            
            foreach (var part in parts)
            {
                var trimmedPart = part.Trim();
                
                // Skip extend clauses as they create new columns, don't reference existing ones
                if (trimmedPart.StartsWith("extend", StringComparison.OrdinalIgnoreCase))
                {
                    // For extend clauses, only look at the right side of assignments for column references
                    ExtractColumnsFromExtendClause(trimmedPart, columns);
                }
                // Skip project clauses as they typically just list columns or create new ones
                else if (trimmedPart.StartsWith("project", StringComparison.OrdinalIgnoreCase))
                {
                    ExtractColumnsFromProjectClause(trimmedPart, columns);
                }
                // For other clauses (where, summarize, etc.), look for column references
                else if (!string.IsNullOrWhiteSpace(trimmedPart))
                {
                    ExtractColumnsFromGenericClause(trimmedPart, columns);
                }
            }

            return columns;
        }

        /// <summary>
        /// Extracts column references from an extend clause (only from the right side of assignments).
        /// </summary>
        private static void ExtractColumnsFromExtendClause(string extendClause, HashSet<string> columns)
        {
            // Remove "extend" keyword
            var clause = Regex.Replace(extendClause, @"^\s*extend\s+", "", RegexOptions.IgnoreCase);
            
            // Split by comma to get individual assignments
            var assignments = clause.Split(',');
            
            foreach (var assignment in assignments)
            {
                var parts = assignment.Split('=');
                if (parts.Length >= 2)
                {
                    // Only look at the right side (the expression) for column references
                    var expression = parts[1].Trim();
                    ExtractColumnsFromExpression(expression, columns);
                }
            }
        }

        /// <summary>
        /// Extracts column references from a project clause.
        /// </summary>
        private static void ExtractColumnsFromProjectClause(string projectClause, HashSet<string> columns)
        {
            // Remove "project" keyword
            var clause = Regex.Replace(projectClause, @"^\s*project\s+", "", RegexOptions.IgnoreCase);
            
            // Split by comma to get individual projections
            var projections = clause.Split(',');
            
            foreach (var projection in projections)
            {
                var parts = projection.Split('=');
                if (parts.Length == 1)
                {
                    // Simple column reference (no assignment)
                    var columnName = parts[0].Trim();
                    if (IsValidColumnName(columnName))
                    {
                        columns.Add(columnName);
                    }
                }
                else if (parts.Length >= 2)
                {
                    // Assignment - look at the right side for column references
                    var expression = parts[1].Trim();
                    ExtractColumnsFromExpression(expression, columns);
                }
            }
        }

        /// <summary>
        /// Extracts column references from a generic clause.
        /// </summary>
        private static void ExtractColumnsFromGenericClause(string clause, HashSet<string> columns)
        {
            ExtractColumnsFromExpression(clause, columns);
        }

        /// <summary>
        /// Extracts column references from an expression.
        /// </summary>
        private static void ExtractColumnsFromExpression(string expression, HashSet<string> columns)
        {
            // Remove string literals to avoid extracting words from within strings
            var cleanExpression = RemoveStringLiterals(expression);
            
            // Look for column references (simplified pattern)
            var columnMatches = Regex.Matches(cleanExpression, @"\b([a-zA-Z_][a-zA-Z0-9_]*)\b");
            
            foreach (Match match in columnMatches)
            {
                var word = match.Value;
                // Skip KQL keywords and functions
                if (!IsKqlKeyword(word) && !IsKqlFunction(word) && IsValidColumnName(word))
                {
                    columns.Add(word);
                }
            }
        }

        /// <summary>
        /// Removes string literals from an expression to avoid extracting identifiers from within strings.
        /// </summary>
        private static string RemoveStringLiterals(string expression)
        {
            // Remove single-quoted strings
            expression = Regex.Replace(expression, @"'[^']*'", "''");
            
            // Remove double-quoted strings
            expression = Regex.Replace(expression, @"""[^""]*""", @"""""");
            
            // Remove multi-line strings (```...```)
            expression = Regex.Replace(expression, @"```[^`]*```", "``````");
            
            return expression;
        }

        /// <summary>
        /// Checks if a string is a valid column name (simple validation).
        /// </summary>
        private static bool IsValidColumnName(string name)
        {
            return !string.IsNullOrWhiteSpace(name) && 
                   Regex.IsMatch(name, @"^[a-zA-Z_][a-zA-Z0-9_]*$") &&
                   !IsKqlKeyword(name) && 
                   !IsKqlFunction(name);
        }

        /// <summary>
        /// Attempts to infer the data type from a KQL expression.
        /// </summary>
        private static string InferTypeFromExpression(string expression)
        {
            expression = expression.Trim();

            // DateTime functions
            if (expression.Contains("now()") || expression.Contains("datetime("))
                return "datetime";
            
            // String functions or literals
            if (expression.Contains("strcat(") || expression.Contains("tostring(") || expression.StartsWith("\""))
                return "string";
            
            // Numeric literals or functions
            if (Regex.IsMatch(expression, @"^\d+$"))
                return "int";
            
            if (Regex.IsMatch(expression, @"^\d+\.\d+$"))
                return "real";
            
            // Boolean literals
            if (expression.Equals("true", StringComparison.OrdinalIgnoreCase) || 
                expression.Equals("false", StringComparison.OrdinalIgnoreCase))
                return "bool";

            // Default to dynamic if we can't determine
            return "dynamic";
        }

        /// <summary>
        /// Checks if two Kusto data types are compatible.
        /// </summary>
        private static bool AreTypesCompatible(string sourceType, string targetType, UpdatePolicyValidationConfig config)
        {
            // Exact match
            if (sourceType.Equals(targetType, StringComparison.OrdinalIgnoreCase))
                return true;

            // Dynamic is compatible with everything
            if (sourceType.Equals("dynamic", StringComparison.OrdinalIgnoreCase) || 
                targetType.Equals("dynamic", StringComparison.OrdinalIgnoreCase))
                return true;

            // Numeric type compatibility
            if (!config.EnforceStrictTypeCompatibility)
            {
                var numericTypes = new[] { "int", "long", "real", "decimal" };
                if (numericTypes.Contains(sourceType.ToLower()) && numericTypes.Contains(targetType.ToLower()))
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Checks if a string is a valid managed identity format.
        /// </summary>
        private static bool IsValidManagedIdentity(string managedIdentity)
        {
            // Basic validation for common managed identity formats
            return managedIdentity.Equals("system", StringComparison.OrdinalIgnoreCase) ||
                   Regex.IsMatch(managedIdentity, @"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$") ||
                   Regex.IsMatch(managedIdentity, @"^[a-zA-Z][a-zA-Z0-9-_]*$");
        }

        /// <summary>
        /// Checks if a word is a KQL keyword.
        /// </summary>
        private static bool IsKqlKeyword(string word)
        {
            var keywords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "where", "project", "extend", "summarize", "order", "by", "limit", "take",
                "join", "union", "let", "and", "or", "not", "in", "has", "contains",
                "startswith", "endswith", "between", "ago", "now", "true", "false"
            };
            
            return keywords.Contains(word);
        }

        /// <summary>
        /// Checks if a word is a KQL function.
        /// </summary>
        private static bool IsKqlFunction(string word)
        {
            var functions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "count", "sum", "avg", "min", "max", "tostring", "toint", "toreal", "todatetime",
                "todynamic", "tobool", "tolong", "toint", "todecimal", "toguid", "totimespan",
                "strcat", "strlen", "substring", "split", "parse", "extract", "bin", "floor",
                "ceiling", "round", "abs", "log", "exp", "sqrt", "pow", "now", "ago", "datetime",
                "timespan", "case", "iff", "isnull", "isempty", "isnotnull", "isnotempty"
            };
            
            return functions.Contains(word);
        }
    }

    /// <summary>
    /// Represents the result of update policy validation.
    /// </summary>
    public class UpdatePolicyValidationResult
    {
        public List<string> Errors { get; } = new List<string>();
        public List<string> Warnings { get; } = new List<string>();

        public bool IsValid => !Errors.Any();
        public bool HasWarnings => Warnings.Any();

        public void AddError(string error)
        {
            Errors.Add(error);
        }

        public void AddWarning(string warning)
        {
            Warnings.Add(warning);
        }

        public override string ToString()
        {
            var messages = new List<string>();
            
            if (Errors.Any())
            {
                messages.Add($"Errors: {string.Join(", ", Errors)}");
            }
            
            if (Warnings.Any())
            {
                messages.Add($"Warnings: {string.Join(", ", Warnings)}");
            }

            return messages.Any() ? string.Join("; ", messages) : "Valid";
        }
    }
}
