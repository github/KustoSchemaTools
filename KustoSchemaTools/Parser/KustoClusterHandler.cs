using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using Kusto.Data.Common;
using Newtonsoft.Json;
using System.Data;

namespace KustoSchemaTools
{
    public class KustoClusterHandler
    {
        private readonly ICslQueryProvider _client;
        private readonly ILogger<KustoClusterHandler> _logger;

        public KustoClusterHandler(ICslQueryProvider client, ILogger<KustoClusterHandler> logger)
        {
            _client = client;
            _logger = logger;
        }

        public async Task<Cluster> LoadAsync()
        {
            var cluster = new Cluster();
            _logger.LogInformation("Loading cluster capacity policy...");

            try
            {
                using (var reader = await _client.ExecuteControlCommandAsync(".show cluster policy capacity"))
                {
                    if (reader.Read())
                    {
                        var policyJson = reader["Policy"].ToString();
                        var policy = JsonConvert.DeserializeObject<CapacityPolicy>(policyJson);
                        cluster.CapacityPolicy = policy;
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