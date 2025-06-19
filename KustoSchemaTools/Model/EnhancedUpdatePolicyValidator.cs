using KustoSchemaTools.Model;

namespace KustoSchemaTools.Model
{
    /// <summary>
    /// Enhanced UpdatePolicyValidator that uses the official Kusto parser for more accurate validation.
    /// This demonstrates how to integrate KustoQuerySchemaExtractor with the existing validation system.
    /// </summary>
    public static class EnhancedUpdatePolicyValidator
    {
        /// <summary>
        /// Validates an update policy using the official Kusto parser for more accurate analysis.
        /// </summary>
        public static UpdatePolicyValidationResult ValidatePolicyWithKustoParser(
            UpdatePolicy updatePolicy, 
            Table targetTable, 
            Table? sourceTable, 
            Database database)
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

            // Validate basic properties (same as before)
            ValidateBasicProperties(updatePolicy, result);

            // Validate source table exists (same as before)
            ValidateSourceTable(updatePolicy, database, result);

            // Enhanced validation using Kusto parser
            if (sourceTable != null)
            {
                ValidateQueryWithKustoParser(updatePolicy, targetTable, sourceTable, result);
            }

            return result;
        }

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
        }

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
        /// Uses the Kusto parser to validate the query and check schema compatibility.
        /// </summary>
        private static void ValidateQueryWithKustoParser(
            UpdatePolicy updatePolicy, 
            Table targetTable, 
            Table sourceTable, 
            UpdatePolicyValidationResult result)
        {
            try
            {
                // Validate the query syntax and semantics
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
                if (queryValidation.IsValid && targetTable.Columns != null)
                {
                    ValidateSchemaCompatibilityWithParser(queryValidation, targetTable, result);
                    ValidateColumnReferencesWithParser(queryValidation, sourceTable, result);
                }
            }
            catch (Exception ex)
            {
                result.AddError($"Failed to validate query with Kusto parser: {ex.Message}");
            }
        }

        /// <summary>
        /// Validates schema compatibility using the parser's output schema information.
        /// </summary>
        private static void ValidateSchemaCompatibilityWithParser(
            KustoQueryValidationResult queryValidation, 
            Table targetTable, 
            UpdatePolicyValidationResult result)
        {
            // Check if query output matches target table schema
            foreach (var targetColumn in targetTable.Columns!)
            {
                if (queryValidation.OutputSchema.TryGetValue(targetColumn.Key, out var queryColumnType))
                {
                    if (!AreTypesCompatible(queryColumnType, targetColumn.Value))
                    {
                        result.AddError($"Column '{targetColumn.Key}' type mismatch: query produces '{queryColumnType}' but target table expects '{targetColumn.Value}'");
                    }
                }
                else
                {
                    result.AddWarning($"Target table column '{targetColumn.Key}' is not produced by the query");
                }
            }

            // Check for extra columns in query output
            foreach (var queryColumn in queryValidation.OutputSchema)
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
        private static void ValidateColumnReferencesWithParser(
            KustoQueryValidationResult queryValidation, 
            Table sourceTable, 
            UpdatePolicyValidationResult result)
        {
            if (sourceTable.Columns == null) return;

            foreach (var referencedColumn in queryValidation.ReferencedColumns)
            {
                if (!sourceTable.Columns.ContainsKey(referencedColumn))
                {
                    result.AddError($"Query references column '{referencedColumn}' which does not exist in source table");
                }
            }
        }

        /// <summary>
        /// Checks if two Kusto data types are compatible.
        /// </summary>
        private static bool AreTypesCompatible(string sourceType, string targetType)
        {
            // Exact match
            if (sourceType.Equals(targetType, StringComparison.OrdinalIgnoreCase))
                return true;

            // Dynamic is compatible with everything
            if (sourceType.Equals("dynamic", StringComparison.OrdinalIgnoreCase) || 
                targetType.Equals("dynamic", StringComparison.OrdinalIgnoreCase))
                return true;

            // Numeric type compatibility
            var numericTypes = new[] { "int", "long", "real", "decimal" };
            if (numericTypes.Contains(sourceType.ToLower()) && numericTypes.Contains(targetType.ToLower()))
                return true;

            return false;
        }
    }
}
