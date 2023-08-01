using Kusto.Data.Common;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoDatabaseRetentionAndCacheLoader : IKustoBulkEntitiesLoader
    {
        const string retentionScript = @".show database schema as csl script | where DatabaseSchemaScript contains 'policy retention' and DatabaseSchemaScript startswith '.alter database' | project Retention= strcat(extract('\\""([0-9]*)\\.', 1,DatabaseSchemaScript),'d')";
        const string hotChacheScript = @".show database schema as csl script | where DatabaseSchemaScript contains 'policy caching' and DatabaseSchemaScript startswith '.alter database' | project HotCache= strcat(extract('\\(([0-9]*)\\.', 1,DatabaseSchemaScript),'d');";
        public async Task Load(Database database, string databaseName, KustoClient kusto)
        {
            if(database.DefaultRetentionAndCache == null)
            {
                database.DefaultRetentionAndCache = new();
            }

            var response = await kusto.Client.ExecuteQueryAsync("operations", retentionScript, new ClientRequestProperties());
            database.DefaultRetentionAndCache.Retention = response.ToScalar<string>(); 
            response = await kusto.Client.ExecuteQueryAsync("operations", hotChacheScript, new ClientRequestProperties());
            database.DefaultRetentionAndCache.HotCache = response.ToScalar<string>();
        }
    }
}
