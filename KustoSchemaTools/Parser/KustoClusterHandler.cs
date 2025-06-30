using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using Kusto.Data.Common;
using Newtonsoft.Json;
using System.Data;
using KustoSchemaTools.Parser;

namespace KustoSchemaTools
{
    public class KustoClusterHandler
    {
        private readonly KustoClient _client;
        private readonly ILogger<KustoClusterHandler> _logger;

        public KustoClusterHandler(KustoClient client, ILogger<KustoClusterHandler> logger)
        {
            _client = client;
            _logger = logger;
        }

        public virtual async Task<Cluster> LoadAsync()
        {
            var cluster = new Cluster();
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