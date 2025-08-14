using Kusto.Data;
using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser.KustoLoader;
using KustoSchemaTools.Plugins;
using Microsoft.Extensions.Logging;
using System.Text;

namespace KustoSchemaTools.Parser.KustoWriter
{
    public class DefaultDatabaseWriter : IDBEntityWriter
    {
        public virtual async Task WriteAsync(Database sourceDb, Database targetDb, KustoClient client, ILogger logger)
        {
            var allResults = await UpdatePrimary(sourceDb, targetDb, client, logger);
            allResults.AddRange(await UpdateFollowers(sourceDb, logger));

            // Throw exception if there were any problems.
            var exceptions = allResults
                .Where(itm => itm.Result == "Failed")
                .Select(itm => new Exception($"Execution failed for command:{itm.OperationId} with reason: {itm.Reason}"))
                .ToList();

            if (exceptions.Count == 1)
            {
                throw exceptions[0];
            }
            if (exceptions.Count > 1)
            {
                throw new AggregateException(exceptions);
            }
        }

        internal virtual async Task<List<ScriptExecuteCommandResult>> UpdatePrimary(Database sourceDb, Database targetDb, KustoClient client, ILogger logger)
        {
            // Some changes will be dependent upon each other, causing a race condition when attempting to apply them.
            // As long as the write made some forward progress, keep looping until it stalls.
            logger.LogInformation($"Updating primary database {targetDb.Name}");
            var keepGoing = false;
            var iterationCount = 0;
            var allResults = new List<ScriptExecuteCommandResult>();

            // Iteratively generate changes and apply them until we stop making forward progress.
            do
            {
                iterationCount++;
                var changes = GenerateChanges(targetDb, sourceDb, logger);
                var results = await ApplyChangesToDatabase(targetDb.Name, changes, client, logger);

                // Save the successes
                var successes = results.Where(r => r.Result != "Failed").ToList();
                allResults.AddRange(successes);
                logger.LogInformation($"Iteration {iterationCount}: Successfully applied {successes.Count} out of {results.Count} changes.");

                // Decide whether to loop
                keepGoing = successes.Count < results.Count && successes.Count > 0;
                if (!keepGoing)
                {
                    // add remaining results to the list.
                    allResults.AddRange(results);
                }
            } while (keepGoing);

            // Final status
            Console.WriteLine($"Successfully applied {allResults.Count(r => r.Result != "Failed")} out of {allResults.Count} changes to {targetDb.Name}");
            foreach (var result in allResults)
            {
                Console.WriteLine($"{result.CommandType} ({result.OperationId}): {result.Result} => {result.Reason} ({result.CommandText})");
            }

            return allResults;
        }

        internal virtual async Task<List<ScriptExecuteCommandResult>> UpdateFollowers(Database sourceDb, ILogger logger)
        {
            // Update followers with the latest changes
            var results = new List<ScriptExecuteCommandResult>();

            foreach (var follower in sourceDb.Followers)
            {
                var followerClient = new KustoClient(follower.Key);
                var source = FollowerLoader.LoadFollower(follower.Value.DatabaseName, followerClient);

                var followerChanges = DatabaseChanges.GenerateFollowerChanges(source, follower.Value, logger);

                var followerResults = await ApplyChangesToDatabase(follower.Value.DatabaseName, followerChanges, followerClient, logger);
                results.AddRange(followerResults);

                Console.WriteLine();
                Console.WriteLine($"Follower: {follower.Key}");
                Console.WriteLine("---------------------------------------------------------------------------");
                Console.WriteLine();

                foreach (var result in followerResults)
                {
                    Console.WriteLine($"{result.CommandType} ({result.OperationId}): {result.Result} => {result.Reason} ({result.CommandText})");
                    Console.WriteLine("---------------------------------------------------------------------------");
                }

                Console.WriteLine();
                Console.WriteLine();
            }

            return results;
        }

        internal virtual List<IChange> GenerateChanges(Database targetDb, Database sourceDb, ILogger logger)
        {
            return DatabaseChanges.GenerateChanges(targetDb, sourceDb, targetDb.Name, logger);
        }

        internal virtual async Task<List<ScriptExecuteCommandResult>> ApplyChangesToDatabase(
            string databaseName, List<IChange> changes, KustoClient client, ILogger logger)
        {
            // Filter and sort scripts
            var scripts = changes
                .SelectMany(itm => itm.Scripts)
                .Where(itm => itm.IsValid == true)
                .Where(itm => itm.Order >= 0)
                .OrderBy(itm => itm.Order)
                .ToList();

            logger.LogInformation($"Applying {scripts.Count} scripts to database '{databaseName}'");

            var results = new List<ScriptExecuteCommandResult>();

            // Process scripts in batches, separating synchronous and asynchronous scripts
            var pendingBatch = new List<DatabaseScriptContainer>();

            foreach (var script in scripts)
            {
                if (script.IsAsync)
                {
                    // If we encounter an async script, execute any pending sync scripts first
                    if (pendingBatch.Count != 0)
                    {
                        var batchResults = await ExecutePendingSync(databaseName, client, logger, pendingBatch);
                        results.AddRange(batchResults);
                        pendingBatch.Clear();
                    }

                    // Then execute and record the async script
                    logger.LogInformation($"Executing async script with order {script.Order}");
                    var asyncResult = await ExecuteAsyncCommand(databaseName, client, logger, script);
                    results.Add(asyncResult);
                }
                else
                {
                    // Collect synchronous scripts into a batch
                    pendingBatch.Add(script);
                }
            }

            // Execute any remaining synchronous scripts
            if (pendingBatch.Any())
            {
                var finalBatchResults = await ExecutePendingSync(databaseName, client, logger, pendingBatch);
                results.AddRange(finalBatchResults);
            }

            return results;
        }

        internal virtual async Task<ScriptExecuteCommandResult> ExecuteAsyncCommand(string databaseName, KustoClient client, ILogger logger, DatabaseScriptContainer sc)
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

        internal virtual async Task<List<ScriptExecuteCommandResult>> ExecutePendingSync(
            string databaseName, KustoClient client, ILogger logger, List<DatabaseScriptContainer> scripts)
        {
            // this function will build a single .execute script from all the small scripts provided.
            // Execute script is defined here: https://learn.microsoft.com/en-us/kusto/management/execute-database-script?view=azure-data-explorer
            if (scripts.Count == 0)
            {
                return [];
            }

            var sb = new StringBuilder();
            sb.AppendLine(".execute script with(ContinueOnErrors = true) <|");
            foreach (var sc in scripts)
            {
                sb.AppendLine(sc.Text);
            }

            var script = sb.ToString();
            logger.LogInformation($"Applying batch of {scripts.Count} scripts to database {databaseName}");
            logger.LogDebug($"Script content:\n{script}");

            var result = await client.AdminClient.ExecuteControlCommandAsync(databaseName, script);
            var resultsList = result.As<ScriptExecuteCommandResult>();
            return resultsList;
        }
    }
}
