using Kusto.Data.Common;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using KustoSchemaTools.Plugins;
using Microsoft.Extensions.Logging;

namespace KustoSchemaTools.Parser.KustoWriter
{
    public class ClusterCapacityPolicyWriter : IDBEntityWriter
    {
        private readonly ClusterCapacityPolicy? _capacityPolicy;

        public ClusterCapacityPolicyWriter(ClusterCapacityPolicy? capacityPolicy)
        {
            _capacityPolicy = capacityPolicy;
        }

        public async Task WriteAsync(Database sourceDb, Database targetDb, KustoClient client, ILogger logger)
        {
            if (_capacityPolicy == null)
            {
                logger.LogInformation("No capacity policy defined, skipping cluster capacity policy configuration");
                return;
            }

            try
            {
                logger.LogInformation("Applying cluster capacity policy");
                var script = _capacityPolicy.CreateScript();
                await client.AdminClient.ExecuteControlCommandAsync("", script.Text, new ClientRequestProperties());
                logger.LogInformation("Cluster capacity policy applied successfully");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to apply cluster capacity policy");
                throw;
            }
        }
    }
}
