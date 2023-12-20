using Kusto.Data;
using Kusto.Toolkit;
using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;
using Microsoft.Extensions.Logging;
using System.Text;

namespace KustoSchemaTools.Parser.KustoWriter
{
    public class DefaultDatabaseWriter : IDBEntityWriter
    {
        public async Task WriteAsync(Database sourceDb, Database targetDb, KustoClient client, ILogger logger)
        {
            var changes = DatabaseChanges.GenerateChanges(targetDb, sourceDb, targetDb.Name, logger);
            var results = await ApplyChangesToDatabase(targetDb.Name, changes, client, logger);

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


        private async Task<List<ScriptExecuteCommandResult>> ApplyChangesToDatabase(string databaseName, List<IChange> changes, KustoClient client, ILogger logger)
        {
            var scripts = changes
                .SelectMany(itm => itm.Scripts)
                .Where(itm => itm.Order >= 0)
                .Where(itm => itm.IsValid == true)
               .OrderBy(itm => itm.Order)
                .ToList();

            var sb = new StringBuilder();
            sb.AppendLine(".execute script with(ContinueOnErrors = true) <|");
            foreach (var sc in scripts)
            {
                sb.AppendLine(sc.Text);
            }

            var script = sb.ToString();
            logger.LogInformation($"Applying sript:\n{script}");
            var result = await client.AdminClient.ExecuteControlCommandAsync(databaseName, script);
            return result.As<ScriptExecuteCommandResult>();
        }
    }
}
