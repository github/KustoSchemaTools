using KustoSchemaTools.Model;

namespace KustoSchemaTools.Examples
{
    /// <summary>
    /// Comprehensive examples showing how to use the Kusto query parser library
    /// to extract schema information from KQL queries.
    /// </summary>
    public static class KustoParserExamples
    {
        /// <summary>
        /// Basic example: Extract output schema from a simple project query
        /// </summary>
        public static void BasicSchemaExtraction()
        {
            Console.WriteLine("=== Basic Schema Extraction ===");
            
            // Define the source table schema
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Timestamp", "datetime" },
                { "UserId", "string" },
                { "EventType", "string" },
                { "Data", "dynamic" }
            };

            // Simple project query
            var query = "SourceTable | project EventId, Timestamp, UserId";

            try
            {
                // Extract the output schema
                var outputSchema = KustoQuerySchemaExtractor.ExtractOutputSchema(query, sourceSchema);

                Console.WriteLine($"Query: {query}");
                Console.WriteLine("Output Schema:");
                foreach (var column in outputSchema)
                {
                    Console.WriteLine($"  {column.Key}: {column.Value}");
                }
                // Expected output: EventId: string, Timestamp: datetime, UserId: string
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Advanced example: Extract schema from a query with transformations
        /// </summary>
        public static void AdvancedSchemaExtraction()
        {
            Console.WriteLine("=== Advanced Schema Extraction ===");
            
            // Define a more complex source table schema
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Timestamp", "datetime" },
                { "Count", "int" },
                { "Amount", "real" },
                { "Data", "dynamic" }
            };

            // Complex query with extend, type conversions, and functions
            var query = @"SourceTable 
                         | where Timestamp > ago(1h)
                         | extend 
                             ProcessedTime = now(),
                             CountAsString = tostring(Count),
                             DoubleAmount = Amount * 2,
                             EventAge = now() - Timestamp
                         | project EventId, ProcessedTime, CountAsString, DoubleAmount, EventAge";

            try
            {
                var outputSchema = KustoQuerySchemaExtractor.ExtractOutputSchema(query, sourceSchema);

                Console.WriteLine($"Query: {query}");
                Console.WriteLine("Output Schema:");
                foreach (var column in outputSchema)
                {
                    Console.WriteLine($"  {column.Key}: {column.Value}");
                }
                // Expected: EventId: string, ProcessedTime: datetime, CountAsString: string, DoubleAmount: real, EventAge: timespan
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Example: Extract column references from a query
        /// </summary>
        public static void ColumnReferenceExtraction()
        {
            Console.WriteLine("=== Column Reference Extraction ===");
            
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Timestamp", "datetime" },
                { "Count", "int" },
                { "Amount", "real" },
                { "Data", "dynamic" }
            };

            var query = @"SourceTable 
                         | where EventId != '' and Timestamp > ago(1h) 
                         | extend ProcessedCount = Count * 2 
                         | project EventId, ProcessedCount, Amount";

            try
            {
                var referencedColumns = KustoQuerySchemaExtractor.ExtractColumnReferences(
                    query, "SourceTable", sourceSchema);

                Console.WriteLine($"Query: {query}");
                Console.WriteLine("Referenced Source Columns:");
                foreach (var column in referencedColumns)
                {
                    Console.WriteLine($"  - {column}");
                }
                // Expected: EventId, Timestamp, Count, Amount (ProcessedCount is derived, not referenced)
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Example: Comprehensive query validation with detailed results
        /// </summary>
        public static void ComprehensiveQueryValidation()
        {
            Console.WriteLine("=== Comprehensive Query Validation ===");
            
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Timestamp", "datetime" },
                { "Count", "int" },
                { "Data", "dynamic" }
            };

            // Test with a valid query
            var validQuery = @"SourceTable 
                              | where Timestamp > ago(1h) 
                              | extend ProcessedAt = now() 
                              | project EventId, ProcessedAt, Count";

            Console.WriteLine("Validating VALID query:");
            var result = KustoQuerySchemaExtractor.ValidateQuery(validQuery, sourceSchema);
            Console.WriteLine($"Is Valid: {result.IsValid}");
            if (result.IsValid)
            {
                Console.WriteLine("Output Schema:");
                foreach (var column in result.OutputSchema)
                {
                    Console.WriteLine($"  {column.Key}: {column.Value}");
                }
                Console.WriteLine("Referenced Columns:");
                foreach (var column in result.ReferencedColumns)
                {
                    Console.WriteLine($"  - {column}");
                }
            }

            Console.WriteLine();

            // Test with an invalid query
            var invalidQuery = "SourceTable | project EventId, NonExistentColumn";

            Console.WriteLine("Validating INVALID query:");
            var invalidResult = KustoQuerySchemaExtractor.ValidateQuery(invalidQuery, sourceSchema);
            Console.WriteLine($"Is Valid: {invalidResult.IsValid}");
            if (!invalidResult.IsValid)
            {
                Console.WriteLine("Errors:");
                foreach (var error in invalidResult.Errors)
                {
                    Console.WriteLine($"  - {error}");
                }
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Example: Update Policy Schema Validation
        /// This shows how the parser can be used to validate update policies
        /// </summary>
        public static void UpdatePolicyValidation()
        {
            Console.WriteLine("=== Update Policy Validation Example ===");
            
            // Source table schema
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Timestamp", "datetime" },
                { "RawData", "string" },
                { "Count", "int" }
            };

            // Target table schema (what the update policy should produce)
            var targetSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "ProcessedTime", "datetime" },
                { "ParsedData", "dynamic" },
                { "DoubleCount", "int" }
            };

            // Update policy query
            var updatePolicyQuery = @"SourceTable 
                                     | extend 
                                         ProcessedTime = now(),
                                         ParsedData = parse_json(RawData),
                                         DoubleCount = Count * 2
                                     | project EventId, ProcessedTime, ParsedData, DoubleCount";

            try
            {
                // Validate the update policy query
                var result = KustoQuerySchemaExtractor.ValidateQuery(updatePolicyQuery, sourceSchema);
                
                Console.WriteLine($"Update Policy Query: {updatePolicyQuery}");
                Console.WriteLine($"Query is valid: {result.IsValid}");
                
                if (result.IsValid)
                {
                    Console.WriteLine("Output Schema from Update Policy:");
                    foreach (var column in result.OutputSchema)
                    {
                        Console.WriteLine($"  {column.Key}: {column.Value}");
                    }

                    // Check schema compatibility
                    Console.WriteLine("\nSchema Compatibility Check:");
                    bool isCompatible = true;
                    foreach (var expectedColumn in targetSchema)
                    {
                        if (result.OutputSchema.TryGetValue(expectedColumn.Key, out var actualType))
                        {
                            if (actualType.Equals(expectedColumn.Value, StringComparison.OrdinalIgnoreCase))
                            {
                                Console.WriteLine($"  ✓ {expectedColumn.Key}: {expectedColumn.Value} matches");
                            }
                            else
                            {
                                Console.WriteLine($"  ✗ {expectedColumn.Key}: expected {expectedColumn.Value}, got {actualType}");
                                isCompatible = false;
                            }
                        }
                        else
                        {
                            Console.WriteLine($"  ✗ {expectedColumn.Key}: missing from output");
                            isCompatible = false;
                        }
                    }

                    Console.WriteLine($"\nOverall compatibility: {(isCompatible ? "✓ Compatible" : "✗ Incompatible")}");
                }
                else
                {
                    Console.WriteLine("Errors:");
                    foreach (var error in result.Errors)
                    {
                        Console.WriteLine($"  - {error}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Run all examples
        /// </summary>
        public static void RunAllExamples()
        {
            Console.WriteLine("Kusto Query Parser Library Examples");
            Console.WriteLine("=====================================");
            Console.WriteLine();

            BasicSchemaExtraction();
            AdvancedSchemaExtraction();
            ColumnReferenceExtraction();
            ComprehensiveQueryValidation();
            UpdatePolicyValidation();

            Console.WriteLine("All examples completed!");
        }
    }
}
