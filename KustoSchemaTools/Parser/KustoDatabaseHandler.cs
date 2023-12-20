using Kusto.Data;
using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Text;

namespace KustoSchemaTools.Parser
{
    public class KustoDatabaseHandler : IDatabaseHandler
    {
        public KustoDatabaseHandler(string clusterUrl, string databaseName, ILogger<KustoDatabaseHandler> logger, List<IKustoBulkEntitiesLoader> reader, List<IDBEntityWriter> writer)
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
        public ILogger<KustoDatabaseHandler> Logger { get; }
        public List<IKustoBulkEntitiesLoader> Reader { get; }
        public List<IDBEntityWriter> Writer { get; }
        public KustoClient Client { get; }

        public async Task<Database> LoadAsync()
        {
            var database = new Database{ Name = DatabaseName };
            foreach (var plugin in Reader)
            {
                await plugin.Load(database, DatabaseName, Client);

            }
            return database;
        }
        public async Task WriteAsync(Database database)
        {
            var targetDb = await LoadAsync();

            foreach (var plugin in Writer)
            {
                await plugin.WriteAsync(database, targetDb, Client, Logger);
            }
        }
        
    }


}
