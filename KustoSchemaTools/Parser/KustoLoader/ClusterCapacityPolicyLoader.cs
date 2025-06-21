using Kusto.Data.Common;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;
using Newtonsoft.Json;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class ClusterCapacityPolicyLoader : IKustoBulkEntitiesLoader
    {
        const string query = @"
            .show cluster policy capacity
| project CapacityPolicy = todynamic(Policy)
        ";

        public async Task Load(Database database, string databaseName, KustoClient client)
        {
            try
            {
                var response = await client.Client.ExecuteQueryAsync("", query, new ClientRequestProperties());
                var capacityPolicyData = response.ToScalar<string>();

                if (!string.IsNullOrEmpty(capacityPolicyData))
                {
                    var capacityPolicy = JsonConvert.DeserializeObject<ClusterCapacityPolicy>(capacityPolicyData);
                    // Store in database for comparison if needed
                    // For now, we don't need to store it as we're only applying changes
                }
            }
            catch (Exception)
            {
                // Capacity policy might not exist or access might be denied
                // This is not critical for the loading process
            }
        }
    }
}
