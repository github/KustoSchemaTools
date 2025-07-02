using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using Kusto.Data.Common;
using Newtonsoft.Json;
using KustoSchemaTools.Parser;

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

            try
            {
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
            }
            catch (System.Exception ex)
            {
                _logger.LogError(ex, "Failed to load cluster capacity policy.");
            }

            return cluster;
        }
    }
}