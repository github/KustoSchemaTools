using FlowLoop.Shared.Kusto;
using Kusto.Data;
using KustoSchemaRollout.Model;
using KustoSchemaTools.Changes;
using KustoSchemaTools.Plugins;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Text;

namespace KustoSchemaTools.Parser
{
    public class KustoDatabaseHandler : IDatabaseHandler
    {
        public KustoDatabaseHandler(string operationsClusterUrl, string clusterUrl, string databaseName, ILogger<KustoDatabaseHandler> logger, List<IKustoBulkEntitiesLoader> plugins)
        {
            OperationsClusterUrl = operationsClusterUrl;
            ClusterUrl = clusterUrl;
            DatabaseName = databaseName;
            Logger = logger;
            Plugins = plugins;
        }

        public string OperationsClusterUrl { get; }
        public string ClusterUrl { get; }
        public string DatabaseName { get; }
        public ILogger<KustoDatabaseHandler> Logger { get; }
        public List<IKustoBulkEntitiesLoader> Plugins { get; }

        public async Task<Database> LoadAsync()
        {
            var client = new KustoClient(OperationsClusterUrl);
            var database = new Database();
            foreach (var plugin in Plugins)
            {
                plugin.Load(database, DatabaseName, client);

            }
            return database;
        }
        public async Task WriteAsync(Database database)
        {
            var targetDb = await LoadAsync();
            var changes = DatabaseChanges.GenerateChanges(targetDb, database, targetDb.Name, Logger);
            var results = await ApplyChangesToDatabase(changes);

            foreach (var result in results)
            {
                Console.WriteLine($"{result.CommandType} ({result.OperationId}): {result.Result} => {result.Reason} ({result.CommandText})");
                Console.WriteLine("---------------------------------------------------------------------------");
            }
            var exs = results.Where(itm => itm.Result == "Failed").Select(itm => new Exception($"Execution failed for command \n{itm.CommandText} \n with reason\n{itm.Reason}")).ToList();
            if (exs.Count == 1)
            {
                throw exs[0];
            }
            if (exs.Count > 1)
            {
                throw new AggregateException(exs);
            }
        }


        private async Task<List<ScriptExecuteCommandResult>> ApplyChangesToDatabase(List<IChange> changes)
        {
            var scripts = changes
                .SelectMany(itm => itm.Scripts)
                .Where(itm => itm.Order >= 0)
                .Where(itm => itm.IsValid == true)
                .ToList();

            var client = new KustoClient(ClusterUrl);

            var sb = new StringBuilder();
            sb.AppendLine(".execute script with(ContinueOnErrors = true) <|");
            foreach (var sc in scripts)
            {
                sb.AppendLine(sc.Text);
            }

            var script = sb.ToString();
            Logger.LogInformation($"Applying sript:\n{script}");
            var result = await client.AdminClient.ExecuteControlCommandAsync(DatabaseName, script);
            return result.As<ScriptExecuteCommandResult>();
        }
    }

    
}
