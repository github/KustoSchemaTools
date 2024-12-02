using Kusto.Data;
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

            var grouped = changes.GroupBy(itm => itm.Cluster).ToList();
            var results = new List<ScriptExecuteCommandResult>();
            foreach (var group in grouped)
            {
                var cluster = group.Key;
                var clusterChanges = group.ToList();
                var clusterClient = new KustoClient(cluster);
                var clusterResults = await ApplyChangesToDatabase(targetDb.Name, clusterChanges, clusterClient, logger);
                foreach (var result in clusterResults)
                {
                    Console.WriteLine($"{result.CommandType} ({result.OperationId}): {result.Result} => {result.Reason} ({result.CommandText})");
                    Console.WriteLine("---------------------------------------------------------------------------");
                }
                results.AddRange(clusterResults);
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

            var results = new List<ScriptExecuteCommandResult>();
            var batch = new List<DatabaseScriptContainer>();
            foreach (var sc in scripts)
            {
                if (sc.IsAsync == false)
                {
                    batch.Add(sc);
                    continue;
                }
                else
                {
                    var batchResults = await ExecutePendingSync(databaseName, client, logger, batch);
                    results.AddRange(batchResults);
                    var asyncResult = await ExecuteAsyncCommand(databaseName, client, logger, sc);
                    results.Add(asyncResult);
                }
            }
            var finalBatchResults = await ExecutePendingSync(databaseName, client, logger, batch);
            results.AddRange(finalBatchResults);
            return results;

        }

        private async Task<ScriptExecuteCommandResult> ExecuteAsyncCommand(string databaseName, KustoClient client, ILogger logger, DatabaseScriptContainer sc)
        {
            var interval = TimeSpan.FromSeconds(5);
            var iterations = (int)(TimeSpan.FromHours(1) / interval);
            var result = await client.AdminClient.ExecuteControlCommandAsync(databaseName, sc.Text);
            var operationId = result.ToScalar<Guid>();
            var finalState = false;
            string monitoringCommand = $".show operations | where OperationId ==  '{operationId}' " +
                "| summarize arg_max(LastUpdatedOn, *) by OperationId " +
                "| project OperationId, CommandType = Operation, Result = State, Reason = Status";
            int cnt = 0;
            while (!finalState)
            {
                if(cnt++ >= iterations)
                {
                    finalState = true;
                }

                logger.LogInformation($"Waiting for operation {operationId} to complete... current iteration: {cnt}/{iterations}");
                var monitoringResult =  client.Client.ExecuteQuery(databaseName, monitoringCommand, new Kusto.Data.Common.ClientRequestProperties());
                var operationState = monitoringResult.As<ScriptExecuteCommandResult>().FirstOrDefault();
                
                if (operationState != null && operationState?.IsFinal() == true)
                {
                    operationState.CommandText = sc.Text;
                    return operationState;
                }
                await Task.Delay(interval);
            }
            throw new Exception("Operation did not complete in a reasonable time");
        }

        private static async Task<List<ScriptExecuteCommandResult>> ExecutePendingSync(string databaseName, KustoClient client, ILogger logger, List<DatabaseScriptContainer> scripts)
        {
            if(scripts.Any() == false)
            {
                return new List<ScriptExecuteCommandResult>();
            } 
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
