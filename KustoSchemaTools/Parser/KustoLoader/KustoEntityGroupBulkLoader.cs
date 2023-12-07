using Kusto.Data.Common;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoEntityGroupBulkLoader : IKustoBulkEntitiesLoader
    {
        const string LoadEntityGroups = ".show entity_groups | project EntityName = Name, parse_json(Entities) | mv-apply Entities to typeof(string) on (extend Cluster = extract('cluster\\\\([\"|\\'](.*?)[\"|\\']', 1,Entities) | extend Database = extract('database\\\\([\"|\\'](.*?)[\"|\\']', 1,Entities) | extend Body = bag_pack_columns(Cluster, Database) | summarize Body=make_list(Body) by EntityName)";
        
        public async Task Load(Database database, string databaseName, KustoClient client)
        {
            var tablesResult = await client.Client.ExecuteQueryAsync(databaseName, LoadEntityGroups, new ClientRequestProperties());
            var entities = tablesResult.As<EnitityLoader<List<Entity>>>().ToDictionary(itm => itm.EntityName, itm => itm.Body);
            database.EntityGroups = entities;
        }

    }
}
