using KustoSchemaTools.Model;
using KustoSchemaTools.Parser.KustoLoader;
using KustoSchemaTools.Parser.KustoWriter;
using KustoSchemaTools.Plugins;
using Microsoft.Extensions.Logging;

namespace KustoSchemaTools.Parser
{
    public class KustoDatabaseHandlerFactory<T> where T : Database, new()
    {
        public KustoDatabaseHandlerFactory(ILogger<KustoDatabaseHandler<T>> logger)
        {
            Logger = logger;
        }

        public List<IKustoBulkEntitiesLoader> Reader { get; } = new();
        public List<IDBEntityWriter> Writer { get; } = new();

        public KustoDatabaseHandlerFactory<T> WithPlugin(IKustoBulkEntitiesLoader plugin)
        {
            Reader.Add(plugin);
            return this;
        }

        public KustoDatabaseHandlerFactory<T> WithReader<U>() where U : IKustoBulkEntitiesLoader, new()
        {
            Reader.Add(new U());
            return this;
        }
        public KustoDatabaseHandlerFactory<T> WithPlugin(IDBEntityWriter plugin)
        {
            Writer.Add(plugin);
            return this;
        }

        public KustoDatabaseHandlerFactory<T> WithWriter<U>() where U : IDBEntityWriter, new()
        {
            Writer.Add(new U());
            return this;
        }

        public ILogger<KustoDatabaseHandler<T>> Logger { get; set; }

        public IDatabaseHandler<T> Create(string cluster, string database)
        {
            return new KustoDatabaseHandler<T>(cluster, database, Logger, Reader, Writer);
        }

        public static IDatabaseHandler<T> CreateDefault(string cluster, string database)
        {
            // Create a logger - in a real application this would come from DI
            var loggerFactory = LoggerFactory.Create(builder => { });
            var logger = loggerFactory.CreateLogger<KustoDatabaseHandler<T>>();

            return new KustoDatabaseHandlerFactory<T>(logger)
                .WithReader<KustoDatabasePrincipalLoader>()
                .WithReader<KustoDatabaseRetentionAndCacheLoader>()
                .WithReader<KustoTableBulkLoader>()
                .WithReader<KustoFunctionBulkLoader>()
                .WithReader<KustoMaterializedViewBulkLoader>()
                .WithReader<KustoExternalTableBulkLoader>()
                .WithReader<KustoContinuousExportBulkLoader>()
                .WithReader<KustoEntityGroupBulkLoader>()
                .WithReader<KustoPartitioningPolicyLoader>()
                .WithReader<ClusterCapacityPolicyLoader>()
                .WithReader<DatabaseCleanup>()
                .WithWriter<DefaultDatabaseWriter>()
                .Create(cluster, database);
        }
    }

    // Add static factory for the non-generic version too
    public static class KustoDatabaseHandlerFactory
    {
        public static IDatabaseHandler<Database> Create(string cluster, string database)
        {
            return KustoDatabaseHandlerFactory<Database>.CreateDefault(cluster, database);
        }
    }
}
