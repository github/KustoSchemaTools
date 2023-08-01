using FlowLoop.Shared.Kusto;
using KustoSchemaRollout.Model;
using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser
{
    public class DatabaseCleanup : IKustoBulkEntitiesLoader, IYamlSchemaPlugin
    {
        public Task Load(Database database, string databaseName, KustoClient client)
        {
            CleanUp(database);
            return Task.CompletedTask;
        }

        public Task OnWrite(Database existingDatabase, string basePath)
        {
            return Task.CompletedTask;
        }


        public Task OnLoad(Database existingDatabase, string basePath)
        {
            CleanUp(existingDatabase);
            return Task.CompletedTask;
        }

        public void CleanUp(Database database)
        {

            // Remove retention and cache policies from tables and materialized views if they are the same as the database default
            foreach (var entity in database.Tables)
            {
                var policy = entity.Value.RetentionAndCachePolicy;

                if (policy == null) continue;

                if (policy.Retention == database.DefaultRetentionAndCache.Retention)
                {
                    policy.Retention = null;
                }
                if (policy.HotCache == database.DefaultRetentionAndCache.HotCache)
                {
                    policy.HotCache = null;
                }
                if (policy.HotCache == null && policy.Retention == null)
                {
                    entity.Value.RetentionAndCachePolicy = null;
                }
            }

            foreach (var entity in database.MaterializedViews)
            {
                var policy = entity.Value.RetentionAndCachePolicy;

                if (policy == null) continue;

                if (policy.Retention == database.DefaultRetentionAndCache.Retention)
                {
                    policy.Retention = null;
                }
                if (policy.HotCache == database.DefaultRetentionAndCache.HotCache)
                {
                    policy.HotCache = null;
                }
                if (policy.HotCache == null && policy.Retention == null)
                {
                    entity.Value.RetentionAndCachePolicy = null;
                }
            }

            foreach (var entity in database.Functions)
            {
                entity.Value.Body = entity.Value.Body.PrettifyKql();
            }
            foreach (var entity in database.MaterializedViews)
            {
                entity.Value.Query = entity.Value.Query.PrettifyKql();
            }
            foreach (var up in database.Tables.Values.Where(itm => itm.UpdatePolicies != null).SelectMany(itm => itm.UpdatePolicies))
            {
                up.Query = up.Query.PrettifyKql();
            }
        }

    }
}
