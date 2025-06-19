# Using the Kusto Query Parser Library for Schema Extraction

This document provides comprehensive examples of how to use the official Microsoft Kusto Language Service (`Microsoft.Azure.Kusto.Language`) to extract schema information from KQL (Kusto Query Language) queries.

## Overview

The Kusto query parser library allows you to:
- **Extract output schema** from KQL queries
- **Validate KQL syntax and semantics**
- **Find column references** in queries
- **Detect type transformations** and computed columns
- **Validate update policies** for schema compatibility

## Getting Started

### Prerequisites

Add the Microsoft Kusto Language package to your project:

```xml
<PackageReference Include="Microsoft.Azure.Kusto.Language" Version="12.0.0" />
```

### Basic Usage

```csharp
using Kusto.Language;
using Kusto.Language.Symbols;
using Kusto.Language.Syntax;
using KustoSchemaTools.Model;
```

## Examples

### 1. Basic Schema Extraction

Extract the output schema from a simple query:

```csharp
// Define source table schema
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

// Extract output schema
var outputSchema = KustoQuerySchemaExtractor.ExtractOutputSchema(query, sourceSchema);

foreach (var column in outputSchema)
{
    Console.WriteLine($"{column.Key}: {column.Value}");
}
// Output:
// EventId: string
// Timestamp: datetime
// UserId: string
```

### 2. Advanced Schema Extraction with Transformations

Handle complex queries with type conversions and computed columns:

```csharp
var sourceSchema = new Dictionary<string, string>
{
    { "EventId", "string" },
    { "Timestamp", "datetime" },
    { "Count", "int" },
    { "Amount", "real" },
    { "Data", "dynamic" }
};

var query = @"SourceTable 
             | where Timestamp > ago(1h)
             | extend 
                 ProcessedTime = now(),
                 CountAsString = tostring(Count),
                 DoubleAmount = Amount * 2,
                 EventAge = now() - Timestamp
             | project EventId, ProcessedTime, CountAsString, DoubleAmount, EventAge";

var outputSchema = KustoQuerySchemaExtractor.ExtractOutputSchema(query, sourceSchema);

// Output schema will include:
// EventId: string
// ProcessedTime: datetime  (from now() function)
// CountAsString: string    (from tostring() conversion)
// DoubleAmount: real       (from arithmetic operation)
// EventAge: timespan       (from datetime subtraction)
```

### 3. Column Reference Extraction

Find which source table columns are referenced in a query:

```csharp
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

var referencedColumns = KustoQuerySchemaExtractor.ExtractColumnReferences(
    query, "SourceTable", sourceSchema);

foreach (var column in referencedColumns)
{
    Console.WriteLine($"- {column}");
}
// Output:
// - EventId
// - Timestamp 
// - Count
// - Amount
// Note: ProcessedCount is NOT included as it's a derived column, not a source column reference
```

### 4. Comprehensive Query Validation

Validate queries for syntax and semantic correctness:

```csharp
var sourceSchema = new Dictionary<string, string>
{
    { "EventId", "string" },
    { "Timestamp", "datetime" },
    { "Count", "int" },
    { "Data", "dynamic" }
};

// Valid query
var validQuery = @"SourceTable 
                  | where Timestamp > ago(1h) 
                  | extend ProcessedAt = now() 
                  | project EventId, ProcessedAt, Count";

var result = KustoQuerySchemaExtractor.ValidateQuery(validQuery, sourceSchema);

if (result.IsValid)
{
    Console.WriteLine("Query is valid!");
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

// Invalid query example
var invalidQuery = "SourceTable | project EventId, NonExistentColumn";
var invalidResult = KustoQuerySchemaExtractor.ValidateQuery(invalidQuery, sourceSchema);

if (!invalidResult.IsValid)
{
    Console.WriteLine("Query has errors:");
    foreach (var error in invalidResult.Errors)
    {
        Console.WriteLine($"  - {error}");
    }
}
```

### 5. Update Policy Schema Validation

Validate that an update policy query produces the correct target schema:

```csharp
// Source table schema
var sourceSchema = new Dictionary<string, string>
{
    { "EventId", "string" },
    { "Timestamp", "datetime" },
    { "RawData", "string" },
    { "Count", "int" }
};

// Expected target table schema
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

// Validate the query
var result = KustoQuerySchemaExtractor.ValidateQuery(updatePolicyQuery, sourceSchema);

if (result.IsValid)
{
    // Check schema compatibility
    bool isCompatible = true;
    foreach (var expectedColumn in targetSchema)
    {
        if (result.OutputSchema.TryGetValue(expectedColumn.Key, out var actualType))
        {
            if (actualType.Equals(expectedColumn.Value, StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"✓ {expectedColumn.Key}: {expectedColumn.Value} matches");
            }
            else
            {
                Console.WriteLine($"✗ {expectedColumn.Key}: expected {expectedColumn.Value}, got {actualType}");
                isCompatible = false;
            }
        }
        else
        {
            Console.WriteLine($"✗ {expectedColumn.Key}: missing from output");
            isCompatible = false;
        }
    }

    Console.WriteLine($"Overall compatibility: {(isCompatible ? "✓ Compatible" : "✗ Incompatible")}");
}
```

## API Reference

### `KustoQuerySchemaExtractor` Class

#### `ExtractOutputSchema(string query, Dictionary<string, string>? sourceTableSchema = null)`

Extracts the output schema from a KQL query.

**Parameters:**
- `query`: The KQL query to analyze
- `sourceTableSchema`: Optional dictionary mapping column names to types for the source table

**Returns:** Dictionary of output column names and their inferred types

**Example:**
```csharp
var schema = KustoQuerySchemaExtractor.ExtractOutputSchema(
    "SourceTable | project EventId, ProcessedTime = now()",
    new Dictionary<string, string> { {"EventId", "string"}, {"Timestamp", "datetime"} }
);
// Returns: {"EventId": "string", "ProcessedTime": "datetime"}
```

#### `ExtractColumnReferences(string query, string sourceTableName, Dictionary<string, string>? sourceTableSchema = null)`

Extracts column references from a KQL query.

**Parameters:**
- `query`: The KQL query to analyze
- `sourceTableName`: Name of the source table
- `sourceTableSchema`: Optional schema of the source table

**Returns:** HashSet of column names referenced in the query

#### `ValidateQuery(string query, Dictionary<string, string>? sourceTableSchema = null, string sourceTableName = "SourceTable")`

Validates a KQL query for syntax and semantic correctness.

**Parameters:**
- `query`: The KQL query to validate
- `sourceTableSchema`: Optional schema of the source table
- `sourceTableName`: Name of the source table (default: "SourceTable")

**Returns:** `KustoQueryValidationResult` with errors, warnings, output schema, and referenced columns

### `KustoQueryValidationResult` Class

Properties:
- `bool IsValid`: Whether the query is valid (no errors)
- `bool HasErrors`: Whether there are any errors
- `bool HasWarnings`: Whether there are any warnings
- `List<string> Errors`: List of error messages
- `List<string> Warnings`: List of warning messages
- `Dictionary<string, string> OutputSchema`: Output column schema
- `HashSet<string> ReferencedColumns`: Set of referenced column names

## Supported Data Types

The parser supports all standard Kusto data types:

| Kusto Type | String Representation |
|------------|----------------------|
| `ScalarTypes.String` | "string" |
| `ScalarTypes.Int` | "int" |
| `ScalarTypes.Long` | "long" |
| `ScalarTypes.Real` | "real" |
| `ScalarTypes.Bool` | "bool" |
| `ScalarTypes.DateTime` | "datetime" |
| `ScalarTypes.TimeSpan` | "timespan" |
| `ScalarTypes.Dynamic` | "dynamic" |
| `ScalarTypes.Guid` | "guid" |
| `ScalarTypes.Decimal` | "decimal" |

## Error Handling

The library throws `InvalidOperationException` for:
- Syntax errors in KQL queries
- Semantic errors (e.g., referencing non-existent columns)
- Parser failures

Always wrap calls in try-catch blocks:

```csharp
try
{
    var schema = KustoQuerySchemaExtractor.ExtractOutputSchema(query, sourceSchema);
    // Use schema...
}
catch (InvalidOperationException ex)
{
    Console.WriteLine($"Query validation failed: {ex.Message}");
}
```

## Integration with Update Policy Validation

This parser can be integrated with the existing `UpdatePolicyValidator` to provide more accurate validation:

```csharp
// Enhanced validation using the parser
var enhancedValidator = new EnhancedUpdatePolicyValidator();
var validationResult = enhancedValidator.ValidateUpdatePolicy(
    updatePolicy, 
    targetTable, 
    sourceTable, 
    database
);
```

## Performance Considerations

- The parser creates a temporary database symbol for each validation
- For high-volume scenarios, consider caching database symbols
- Parser validation is more accurate but slower than regex-based approaches
- Use the simpler `ExtractOutputSchema` method when you only need schema information

## Limitations

- Requires the `Microsoft.Azure.Kusto.Language` package
- May not support the very latest KQL language features immediately
- Cross-database queries require additional setup
- Performance overhead compared to simple regex parsing

## See Also

- [Microsoft.Azure.Kusto.Language Documentation](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/api/netfx/kusto-language-service)
- [KQL Language Reference](https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/)
- Update Policy Validation Examples in `KustoParserExamples.cs`
