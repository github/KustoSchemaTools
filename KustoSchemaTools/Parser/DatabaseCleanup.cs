using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser
{
    public class DatabaseCleanup : YamlSchemaPlugin, IKustoBulkEntitiesLoader
    {
        public Task Load(Database database, string databaseName, KustoClient client)
        {
            CleanUp(database);
            return Task.CompletedTask;
        }

        public override Task OnWrite(Database existingDatabase, string basePath)
        {
            return Task.CompletedTask;
        }


        public override Task OnLoad(Database existingDatabase, string basePath)
        {
            CleanUp(existingDatabase);
            return Task.CompletedTask;
        }

        public void CleanUp(Database database)
        {
            foreach (var entity in database.Tables)
            {
                if (entity.Value.Policies == null)
                {
                    entity.Value.Policies = new();
                }
            }
            foreach (var entity in database.MaterializedViews)
            {
                if (entity.Value.Policies == null)
                {
                    entity.Value.Policies = new();
                }
            }

            // Consolidate old policies into the new policy object before processing
            foreach (var entity in database.Tables)
            {
                if (entity.Value.Policies == null)
                {
                    entity.Value.Policies = new();
                }
                var policy = entity.Value.Policies;

                if (entity.Value.RetentionAndCachePolicy != null)
                {
                    if (string.IsNullOrWhiteSpace(entity.Value.RetentionAndCachePolicy.Retention) == false && string.IsNullOrWhiteSpace(policy.Retention))
                    {
                        policy.Retention = entity.Value.RetentionAndCachePolicy.Retention;
                    }

                    if (string.IsNullOrWhiteSpace(entity.Value.RetentionAndCachePolicy.HotCache) == false && string.IsNullOrWhiteSpace(policy.HotCache))
                    {
                        policy.HotCache = entity.Value.RetentionAndCachePolicy.HotCache;
                    }
                    entity.Value.RetentionAndCachePolicy = null;
                }
                if (entity.Value.UpdatePolicies != null && policy.UpdatePolicies == null)
                {
                    policy.UpdatePolicies = entity.Value.UpdatePolicies;
                }

                if (string.IsNullOrWhiteSpace(entity.Value.RowLevelSecurity) == false && string.IsNullOrWhiteSpace(policy.RowLevelSecurity))
                {
                    policy.RowLevelSecurity = entity.Value.RowLevelSecurity;
                }

                policy.RestrictedViewAccess |= entity.Value.RestrictedViewAccess;

                if (policy.Retention == database.DefaultRetentionAndCache.Retention)
                {
                    policy.Retention = null;
                }

                if (policy.HotCache == database.DefaultRetentionAndCache.HotCache)
                {
                    policy.HotCache = null;
                }
            }

            foreach (var entity in database.MaterializedViews)
            {
                if (entity.Value.Policies == null)
                {
                    entity.Value.Policies = new();
                }
                var policy = entity.Value.Policies;

                if (entity.Value.RetentionAndCachePolicy != null)
                {
                    if (string.IsNullOrWhiteSpace(entity.Value.RetentionAndCachePolicy.Retention) == false && string.IsNullOrWhiteSpace(policy.Retention))
                    {
                        policy.Retention = entity.Value.RetentionAndCachePolicy.Retention;
                    }

                    if (string.IsNullOrWhiteSpace(entity.Value.RetentionAndCachePolicy.HotCache) == false && string.IsNullOrWhiteSpace(policy.HotCache))
                    {
                        policy.HotCache = entity.Value.RetentionAndCachePolicy.HotCache;
                    }
                    entity.Value.RetentionAndCachePolicy = null;
                }

                if (string.IsNullOrWhiteSpace(entity.Value.RowLevelSecurity) == false && string.IsNullOrWhiteSpace(policy.RowLevelSecurity))
                {
                    policy.RowLevelSecurity = entity.Value.RowLevelSecurity;
                }


                if (policy.Retention == database.DefaultRetentionAndCache.Retention)
                {
                    policy.Retention = null;
                }

                if (policy.HotCache == database.DefaultRetentionAndCache.HotCache)
                {
                    policy.HotCache = null;
                }

                if (entity.Value.Preformatted == false)
                {
                    // format the query unless the materialized view opts out
                    entity.Value.Query = entity.Value.Query.PrettifyKql();
                }
            }

            foreach(var entity in database.MaterializedViews)
            {
                if (database.MaterializedViews.ContainsKey(entity.Value.Source))
                {
                    entity.Value.Kind = "materialized-view";
                }
            }

            foreach (var entity in database.Functions)
            {
                // format unless the function opts out
                // there are known issues with PrettifyKql function. 
                if (!entity.Value.Preformatted)
                {
                    entity.Value.Body = entity.Value.Body.PrettifyKql();
                }
            }
            foreach (var up in database.Tables.Values.Where(itm => itm.Policies?.UpdatePolicies != null).SelectMany(itm => itm.Policies.UpdatePolicies))
            {
                up.Query = up.Query.PrettifyKql();
            }
        }

    }
}
