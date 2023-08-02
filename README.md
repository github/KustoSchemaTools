# KustoSchemaTools

This C# project provides functionality to work with schemas in Azure Data Explorer (Kusto). You can load a schema from yaml files or a database to the interal data structure. This can be used for creating diffs of two databases as scripts or markdown, and also to write it back to files or update schemas in a database.

A second project "[KustoSchemaToolsAction](https://github.com/github/KustoSchemaToolsAction)" wraps that into CLI tool inside a docker container for usage in GitHub Actions.

## Getting started

The `database` object holds all schema related information for a Kusto database. It can be loaded from, or written to a cluster using the `KustoDatabaseHandler` which can be created by the `KustoDatabaseHandlerFactory`.There are several steps involved for loading a all relevant information from a kusto database into the `database` object. These are covered by different plugins, which can be configured for the `KustoDatabaseHandlerFactory`. 

```csharp
var dbFactory = new KustoDatabaseHandlerFactory(sp.GetService<ILogger<KustoDatabaseHandler>>())
    .WithPlugin<KustoDatabasePrincipalLoader>()
    .WithPlugin<KustoDatabaseRetentionAndCacheLoader>()
    .WithPlugin<KustoTableBulkLoader>()
    .WithPlugin<KustoFunctionBulkLoader>()
    .WithPlugin<KustoMaterializedViewBulkLoader>()
    .WithPlugin<DatabaseCleanup>()
```



 For syncrhonizing it to files, the `YamlDatabaseHandler` and the `YamlDatabaseHandlerFactory` are the right tools. To prevent super large files, there are plugins that handle reading and writing functions, tables and materialized views to separate files and folders. They can be configured for the `YamlDatabaseHandlerFactory`.

```csharp
var yamlFactory = new YamlDatabaseHandlerFactory()
    .WithPlugin(new TablePlugin())
    .WithPlugin(new FunctionPlugin())
    .WithPlugin(new MaterializedViewsPlugin())
    .WithPlugin<DatabaseCleanup>();
```

Additional features can be added with custom plugins. A sample for `table groups`, where the some parts of the schema are defined once, but is applied for several tables can be found in [here](https://github.com/github/KustoSchemaToolsAction/blob/main/KustoSchemaCLI/Plugins/TableGroupPlugin.cs).

The `KustoSchemaHandler` is the central place for synching schemas between yaml and a database. It offers functions for generating changes formatted in markdown, writing a database to yaml files and applying changes from yaml files to a database.

## Supported Features

Currently following features are supported:

* Database
    * Permissions
    * Default Retention
    * Default Hot Cache
* Tables
    * Columns
    * Retention
    * HotCache
    * Update Policies
    * Docstring
    * Folder
* Functions
    * Body
    * Docstring
    * Folder
* Materialized Views
    * Query
    * Retention
    * HotCache
    * Docstring
    * Folder

The `DatabaseCleanup` will remove redundant retention and hotcache definitions. It will also pretty print KQL queries in functions, update policies and materialized views.
