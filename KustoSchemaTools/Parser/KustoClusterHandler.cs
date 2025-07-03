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
        private readonly ICslAdminProvider _adminClient;
        private readonly ILogger<KustoClusterHandler> _logger;
        private readonly string _clusterName;
        private readonly string _clusterUrl;

        public KustoClusterHandler(ICslAdminProvider adminClient, ILogger<KustoClusterHandler> logger, string clusterName, string clusterUrl)
        {
            _adminClient = adminClient;
            _logger = logger;
            _clusterName = clusterName;
            _clusterUrl = clusterUrl;
        }

        public virtual async Task<Cluster> LoadAsync()
        {
            var cluster = new Cluster { Name = _clusterName, Url = _clusterUrl };

            _logger.LogInformation("Loading cluster capacity policy...");

            using (var reader = await _adminClient.ExecuteControlCommandAsync("", ".show cluster policy capacity", new ClientRequestProperties()))
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

            var result = await ExecuteClusterScriptAsync(scripts);
            return result;
        }

        private async Task<List<ScriptExecuteCommandResult>> ExecuteClusterScriptAsync(List<DatabaseScriptContainer> scripts)
        {
            if (scripts.Count == 0)
            {
                _logger.LogInformation("No scripts to execute.");
                return new List<ScriptExecuteCommandResult>();
            }

            var scriptTexts = scripts.Select(script => script.Text);
            var script = ".execute cluster script with(ContinueOnErrors = true) <|" + Environment.NewLine + 
                        string.Join(Environment.NewLine, scriptTexts);

            _logger.LogInformation($"Applying cluster script:\n{script}");
            
            var result = await _adminClient.ExecuteControlCommandAsync("", script);
            return result.As<ScriptExecuteCommandResult>();
        }
    }
}