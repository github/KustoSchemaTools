using Kusto.Cloud.Platform.Utils;
using Kusto.Data.Common;
using KustoSchemaTools.Helpers;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser.KustoLoader
{


    public abstract class KustoBulkEntityLoader<T> : IKustoBulkEntitiesLoader where T : new()
    {
        public KustoBulkEntityLoader(Func<Database, Dictionary<string, T>> selector)
        {
            Selector = selector;
        }

        public Func<Database, Dictionary<string, T>> Selector { get; }

        public virtual async Task Load(Database database, string databaseName, KustoClient kusto)
        {
            var existing = Selector(database);
            foreach (var script in EnumerateScripts())
            {
                var tablesResult = await kusto.Client.ExecuteQueryAsync(databaseName, script, new ClientRequestProperties());
                var entities = tablesResult.As<EnitityLoader<T>>().ToDictionary(itm => itm.EntityName, itm => itm.Body);
                foreach (var entity in entities)
                {
                    existing.Merge(entity.Value, entity.Key);
                }
            }
        }

        protected abstract IEnumerable<string> EnumerateScripts();
    }
}
