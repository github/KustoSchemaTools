# Update Policy Validation Configuration

## Overview

The `UpdatePolicyValidationConfig` class provides configuration options to control the behavior of update policy validation, specifically around type compatibility checking.

## Configuration Options

### EnforceStrictTypeCompatibility

- **Type**: `bool`
- **Default**: `false`
- **Description**: Controls whether strict type compatibility is enforced during validation.

#### When `false` (Default Behavior)
- Allows implicit conversions between numeric types (`int`, `long`, `real`, `decimal`)
- Provides backward compatibility with existing validation behavior
- More permissive, suitable for most use cases

#### When `true` (Strict Mode)
- Requires exact type matches for all columns except `dynamic`
- Rejects any implicit numeric type conversions
- Provides stricter validation for scenarios requiring precise type safety

## Usage Examples

### Basic Usage (Default Permissive Mode)

```csharp
// Uses default configuration - allows numeric type conversions
var result = UpdatePolicyValidator.ValidatePolicy(
    updatePolicy, 
    targetTable, 
    sourceTable, 
    database);
```

### Explicit Default Configuration

```csharp
// Explicitly use default permissive configuration
var result = UpdatePolicyValidator.ValidatePolicy(
    updatePolicy, 
    targetTable, 
    sourceTable, 
    database, 
    UpdatePolicyValidationConfig.Default);
```

### Strict Type Validation

```csharp
// Use strict configuration - rejects numeric type conversions
var result = UpdatePolicyValidator.ValidatePolicy(
    updatePolicy, 
    targetTable, 
    sourceTable, 
    database, 
    UpdatePolicyValidationConfig.Strict);
```

### Custom Configuration

```csharp
// Create custom configuration
var customConfig = new UpdatePolicyValidationConfig
{
    EnforceStrictTypeCompatibility = true  // Enable strict mode
};

var result = UpdatePolicyValidator.ValidatePolicy(
    updatePolicy, 
    targetTable, 
    sourceTable, 
    database, 
    customConfig);
```

## Type Compatibility Rules

### Always Compatible (Both Modes)
- Exact type matches: `int` → `int`, `string` → `string`
- Dynamic type compatibility: `dynamic` is compatible with any type
- Null/empty validation bypassed when table schemas are not defined

### Default Mode (EnforceStrictTypeCompatibility = false)
- **Numeric Types**: `int`, `long`, `real`, `decimal` are all mutually compatible
- **Example**: Query producing `real` can target column expecting `int`

### Strict Mode (EnforceStrictTypeCompatibility = true)
- **Numeric Types**: Each numeric type must match exactly
- **Example**: Query producing `real` cannot target column expecting `int`

## Example Scenarios

### Scenario 1: Numeric Conversion

```csharp
// Source query: SourceTable | project Count = real(IntColumn)
// Target table has: Count int

// Default mode: ✅ Valid (allows int ↔ real conversion)
// Strict mode:  ❌ Invalid (requires exact type match)
```

### Scenario 2: Non-Numeric Types

```csharp
// Source query: SourceTable | project EventTime = toString(DateTimeColumn)  
// Target table has: EventTime datetime

// Default mode: ❌ Invalid (no string ↔ datetime compatibility)
// Strict mode:  ❌ Invalid (no string ↔ datetime compatibility)
```

### Scenario 3: Dynamic Type

```csharp
// Source query: SourceTable | project Data = todynamic(StringColumn)
// Target table has: Data string

// Default mode: ✅ Valid (dynamic compatible with everything)
// Strict mode:  ✅ Valid (dynamic compatible with everything)
```

## When to Use Each Mode

### Default Mode (Recommended)
- **Most common use case** for general update policy validation
- **Backward compatible** with existing validation behavior
- **Flexible** for scenarios where numeric precision differences are acceptable
- **Suitable for** data transformation pipelines where type conversions are common

### Strict Mode
- **High precision requirements** where exact type matching is critical
- **Strict data governance** environments requiring precise schema compliance
- **Development/testing** environments to catch potential type issues early
- **Migration scenarios** where you want to ensure exact schema matching

## Migration Guide

### Existing Code
```csharp
// Existing calls remain unchanged and use default permissive behavior
var result = UpdatePolicyValidator.ValidatePolicy(updatePolicy, targetTable, sourceTable, database);
```

### Adding Strict Validation
```csharp
// To enable strict validation, add the config parameter
var result = UpdatePolicyValidator.ValidatePolicy(
    updatePolicy, 
    targetTable, 
    sourceTable, 
    database, 
    UpdatePolicyValidationConfig.Strict);
```

## Best Practices

1. **Start with Default Mode**: Begin with the default permissive mode for existing projects
2. **Test with Strict Mode**: Use strict mode in development to identify potential type issues
3. **Document Your Choice**: Clearly document which mode you're using in your validation code
4. **Consider Context**: Choose based on your data quality requirements and tolerance for type conversions
5. **Gradual Migration**: When migrating to strict mode, validate existing policies first to identify issues

## Error Messages

### Default Mode
```
// Only shows errors for truly incompatible types
Column 'EventTime' type mismatch: query produces 'string' but target table expects 'datetime'
```

### Strict Mode
```
// Shows errors for any type mismatch, including numeric conversions
Column 'Count' type mismatch: query produces 'real' but target table expects 'int'
```

## Backward Compatibility

- **Existing code continues to work unchanged** - all existing calls use default permissive mode
- **No breaking changes** - new configuration parameter is optional
- **Preserves existing behavior** - default mode maintains the same validation logic as before the feature flag was added
