using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;
using Microsoft.Extensions.Logging;

namespace KustoSchemaTools.Parser
{
    public class KustoDatabaseHandler<T> : IDatabaseHandler<T> where T: Database, new()
    {
        public KustoDatabaseHandler(string clusterUrl, string databaseName, ILogger<KustoDatabaseHandler<T>> logger, List<IKustoBulkEntitiesLoader> reader, List<IDBEntityWriter> writer)
        {
            ClusterUrl = clusterUrl;
            DatabaseName = databaseName;
            Logger = logger;
            Reader = reader;
            Writer = writer;
            Client = new KustoClient(ClusterUrl);
        }

        public string ClusterUrl { get; }
        public string DatabaseName { get; }
        public ILogger<KustoDatabaseHandler<T>> Logger { get; }
        public List<IKustoBulkEntitiesLoader> Reader { get; }
        public List<IDBEntityWriter> Writer { get; }
        public KustoClient Client { get; }

        public async Task<T> LoadAsync()
        {
            var database = new T { Name = DatabaseName };
            foreach (var plugin in Reader)
            {
                await plugin.Load(database, DatabaseName, Client);

            }
            return database;
        }
        public async Task WriteAsync(T database)
        {
            var targetDb = await LoadAsync();

            foreach (var plugin in Writer)
            {
                await plugin.WriteAsync(database, targetDb, Client, Logger);
            }
        }
        
    }

    public class KustoDatabaseHandler : KustoDatabaseHandler<Database>
    {
        public KustoDatabaseHandler(string clusterUrl, string databaseName, ILogger<KustoDatabaseHandler> logger, List<IKustoBulkEntitiesLoader> reader, List<IDBEntityWriter> writer) : base(clusterUrl, databaseName, logger, reader, writer)
        {
        }
    }
}
