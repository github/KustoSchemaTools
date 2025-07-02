using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Kusto.Data.Common;
using Newtonsoft.Json;
using KustoSchemaTools.Parser;
using KustoSchemaTools.Changes;
using Kusto.Data;

namespace KustoSchemaTools
{
    public class KustoClusterHandler
    {
        private readonly KustoClient _client;
        private readonly ILogger<KustoClusterHandler> _logger;
        private readonly string _clusterName;
        private readonly string _clusterUrl;

        public KustoClusterHandler(KustoClient client, ILogger<KustoClusterHandler> logger, string clusterName, string clusterUrl)
        {
            _client = client;
            _logger = logger;
            _clusterName = clusterName;
            _clusterUrl = clusterUrl;
        }

        public virtual async Task<Cluster> LoadAsync()
        {
            var cluster = new Cluster { Name = _clusterName, Url = _clusterUrl };

            _logger.LogInformation("Loading cluster capacity policy...");

            using (var reader = await _client.AdminClient.ExecuteControlCommandAsync("", ".show cluster policy capacity", new ClientRequestProperties()))
            {
                if (reader.Read())
                {
                    var policyJson = reader["Policy"]?.ToString();
                    if (!string.IsNullOrEmpty(policyJson))
                    {
                        var policy = JsonConvert.DeserializeObject<ClusterCapacityPolicy>(policyJson);
                        cluster.CapacityPolicy = policy;
                    }
                }
            }

            return cluster;
        }

        public virtual async Task<List<ScriptExecuteCommandResult>> WriteAsync(ClusterChangeSet changeSet)
        {
            var scripts = changeSet.Changes
                .SelectMany(itm => itm.Scripts)
                .Where(itm => itm.Order >= 0)
                .Where(itm => itm.IsValid == true)
                .OrderBy(itm => itm.Order)
                .ToList();

            var results = new List<ScriptExecuteCommandResult>();

            foreach (var script in scripts)
            {
                var asyncResult = await ExecuteAsyncCommand(script.Text);
                results.Add(asyncResult);
            }

            return results;
        }

        private async Task<ScriptExecuteCommandResult> ExecuteAsyncCommand(string scriptText)
        {
            var interval = TimeSpan.FromSeconds(5);
            var iterations = (int)(TimeSpan.FromHours(1) / interval);
            var result = await _client.AdminClient.ExecuteControlCommandAsync("", scriptText);
            var operationId = result.ToScalar<Guid>();
            var finalState = false;
            string monitoringCommand = $".show operations | where OperationId ==  '{operationId}' " +
                "| summarize arg_max(LastUpdatedOn, *) by OperationId " +
                "| project OperationId, CommandType = Operation, Result = State, Reason = Status";
            int cnt = 0;
            while (!finalState)
            {
                if (cnt++ >= iterations)
                {
                    finalState = true;
                }

                _logger.LogInformation($"Waiting for operation {operationId} to complete... current iteration: {cnt}/{iterations}");
                var monitoringResult = _client.Client.ExecuteQuery(monitoringCommand, new ClientRequestProperties());
                var operationState = monitoringResult.As<ScriptExecuteCommandResult>().FirstOrDefault();

                if (operationState != null && operationState?.IsFinal() == true)
                {
                    operationState.CommandText = scriptText;
                    return operationState;
                }
                await Task.Delay(interval);
            }
            throw new Exception("Operation did not complete in a reasonable time");
        }
    }
}