# KustoSchemaTools

This C# project provides functionality to work with schemas in Azure Data Explorer (Kusto). You can load a schema from yaml files or a database to the interal data structure. This can be used for creating diffs of two databases as scripts or markdown, and also to write it back to files or update schemas in a database.

A second project "[KustoSchemaToolsAction](https://github.com/github/KustoSchemaToolsAction)" wraps that into a CLI tool inside a docker container for usage in GitHub Actions.

## Getting started

### Database management

The `database` object holds all schema related information for a Kusto database. It can be loaded from, or written to a cluster using the `KustoDatabaseHandler` which can be created by the `KustoDatabaseHandlerFactory`. There are several steps involved for loading all relevant information from a kusto database into the `database` object. These are covered by different plugins, which can be configured for the `KustoDatabaseHandlerFactory`. 

```csharp
var dbFactory = new KustoDatabaseHandlerFactory(sp.GetService<ILogger<KustoDatabaseHandler>>())
    .WithPlugin<KustoDatabasePrincipalLoader>()
    .WithPlugin<KustoDatabaseRetentionAndCacheLoader>()
    .WithPlugin<KustoTableBulkLoader>()
    .WithPlugin<KustoFunctionBulkLoader>()
    .WithPlugin<KustoMaterializedViewBulkLoader>()
    .WithPlugin<DatabaseCleanup>()
```



 For synchronizing it to files, the `YamlDatabaseHandler` and the `YamlDatabaseHandlerFactory` are the right tools. To prevent super large files, there are plugins that handle reading and writing functions, tables and materialized views to separate files and folders. They can be configured for the `YamlDatabaseHandlerFactory`.

```csharp
var yamlFactory = new YamlDatabaseHandlerFactory()
    .WithPlugin(new TablePlugin())
    .WithPlugin(new FunctionPlugin())
    .WithPlugin(new MaterializedViewsPlugin())
    .WithPlugin<DatabaseCleanup>();
```

Additional features can be added with custom plugins. A sample for `table groups`, where some parts of the schema are defined once, but are applied for several tables can be found in [here](https://github.com/github/KustoSchemaToolsAction/blob/main/KustoSchemaCLI/Plugins/TableGroupPlugin.cs).

The `KustoSchemaHandler` is the central place for synching schemas between yaml and a database. It offers functions for generating changes formatted in markdown, writing a database to yaml files and applying changes from yaml files to a database.

### Cluster configuration management

Cluster configuration changes are handled by the `KustoClusterOrchestrator`. Currently supported features include [`Capacity Policies`](https://learn.microsoft.com/en-us/kusto/management/capacity-policy?view=azure-data-explorer) and [`Workload Groups`](https://learn.microsoft.com/en-us/kusto/management/workload-groups?view=azure-data-explorer). The orchestrator expects a file path to a configuration file. A key design principle is that you only need to specify the properties you wish to set or change. Any property omitted in your policy file will be ignored, preserving its current value on the cluster.
A sample file could look like this:

```yaml
connections:
- name: test
  url: test.eastus
  capacityPolicy:
    ingestionCapacity:
      clusterMaximumConcurrentOperations: 512
      coreUtilizationCoefficient: 0.75
    extentsMergeCapacity:
      minimumConcurrentOperationsPerNode: 1
      maximumConcurrentOperationsPerNode: 3
    extentsPurgeRebuildCapacity:
      maximumConcurrentOperationsPerNode: 1
  workloadGroups:
  - workloadGroupName: DataScience
    workloadGroupPolicy:
      requestRateLimitsEnforcementPolicy:
        commandsEnforcementLevel: Cluster
```

The `KustoClusterOrchestrator` coordinates between cluster handlers to manage cluster configuration changes:

1. **Loading Configuration**: Uses `YamlClusterHandler` to parse the YAML configuration file and load the desired cluster state
2. **Reading Current State**: Uses `KustoClusterHandler` to connect to each live cluster and retrieve the current capacity policy and workload group settings
3. **Generating Changes**: Compares the desired state (from YAML) with the current state (from Kusto) to identify differences
4. **Creating Scripts**: Generates the necessary Kusto control commands (like `.alter-merge cluster policy capacity` and `.create-or-alter workload_group`) to apply the changes
5. **Applying Updates**: Executes the generated scripts against the live clusters to synchronize them with the desired configuration

Currently no plugins are supported. The orchestrator expects all cluster configuration in a central file.

## Validation Features

### Column Order Validation

When modifying table schemas, the system can optionally validate that new columns are appended to the end of the column definition. This validation prevents update policy failures that occur when Kusto preserves column ordinal positions after ALTER TABLE operations.

**⚠️ Note**: Column order validation is **disabled by default** to preserve existing behavior. Enable it explicitly when needed.

**Validation Rules:**

* New columns added at the end of existing columns: Pass
* New table creation with any column order: Pass (no baseline to compare)
* Existing columns positioned after new columns: Fail with detailed error
* Reordering of existing columns: Fail

**Error Handling:**

Validation failures appear in the diff output as CAUTION-level comments with:
- Description of the violation
- Names of affected columns (new and misplaced)
- Technical explanation of why this matters
- Required remediation steps

The validation failure will block deployment (FailsRollout=true).

**Enabling Validation:**

There are several ways to enable column order validation:

1. **Environment Variable** (recommended for CI/CD):
   ```bash
   export KUSTO_ENABLE_COLUMN_VALIDATION=true
   ```

2. **Programmatically**:
   ```csharp
   // Enable validation via settings
   var settings = ValidationSettings.WithColumnOrderValidation();
   var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "MyDB", logger, settings);
   
   // Or from environment variables
   var settings = ValidationSettings.FromEnvironment();
   var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "MyDB", logger, settings);
   ```

3. **Default behavior** (validation disabled):
   ```csharp
   // This will NOT apply column order validation
   var changes = DatabaseChanges.GenerateChanges(oldDb, newDb, "MyDB", logger);
   ```

**Example Scenarios:**

Invalid configuration (existing column after new column):

```yaml
Columns:
  ExistingColumn1: string
  NewColumn: int          # New column inserted
  ExistingColumn2: bool   # Existing column - causes validation failure
```

Valid configuration (new columns appended):

```yaml
Columns:
  ExistingColumn1: string
  ExistingColumn2: bool
  NewColumn: int          # New column appended at end
```

## Supported Features

Currently following features are supported:

* Cluster
    * Capacity Policies
    * Workload Groups
* Database
    * Permissions
    * Default Retention
    * Default Hot Cache
* Tables
    * Columns
    * Column Order Validation
    * Retention
    * HotCache
    * Update Policies
    * Docstring
    * Folder
* Functions
    * Body
    * Docstring
    * Folder
    * Preformatted
* Materialized Views
    * Query
    * Retention
    * HotCache
    * Docstring
    * Folder
    * Preformatted
* External Tables (managed identity/impersonation only)
    * Storage / Delta / SQL
    * Folder
    * Docstring
* Continuous Exports
* Entity Groups
* Deleting existing items using deletions in the database definition
    * Tables
    * Columns
    * Functions
    * Materialized Views
    * Extenal Tables
    * Continuous Exports

The `DatabaseCleanup` will remove redundant retention and hotcache definitions. 
It will also pretty print KQL queries in functions (unless the `preformatted` feature is used) , update policies, materialized views and continuous exports.
