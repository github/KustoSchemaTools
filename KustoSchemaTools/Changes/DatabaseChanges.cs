﻿using Kusto.Language;
using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Text;

namespace KustoSchemaTools.Changes
{
    public class DatabaseChanges
    {
        public static List<IChange> GenerateChanges(Database oldState, Database newState, string name, ILogger log)
        {

            var result = new List<IChange>();

            var otherFromScripts = new List<DatabaseScriptContainer>();
            if (oldState != null)
            {
                if (oldState.Scripts != null)
                    otherFromScripts.AddRange(oldState.Scripts.Select(itm => new DatabaseScriptContainer(itm, "DatabaseScript")));
                if (oldState.DefaultRetentionAndCache != null)
                    otherFromScripts.AddRange(oldState.DefaultRetentionAndCache.CreateScripts(name, "database"));
            }

            var otherToScripts = new List<DatabaseScriptContainer>();
            if (newState.Scripts != null)
                otherToScripts.AddRange(newState.Scripts.Select(itm => new DatabaseScriptContainer(itm, "DatabaseScript")));
            if (newState.DefaultRetentionAndCache != null)
                otherToScripts.AddRange(newState.DefaultRetentionAndCache.CreateScripts(name, "database"));

            if (otherToScripts.Count > 0)
            {
                var c = new ScriptCompareChange("Database", new GenericBaseEntity(otherFromScripts), new GenericBaseEntity(otherToScripts));
                if (c.Scripts.Any())
                {
                    result.Add(new Heading("Database Changes"));
                    result.Add(c);
                }
            }
            if (oldState == null)
            {
                oldState = new Database();
            }

            result.AddRange(GeneratePermissionChanges(oldState, newState, name, log));

            result.AddRange(GenerateDeletions(oldState, newState.Deletions, log));

            result.AddRange(GenerateScriptCompareChanges(oldState, newState, db => db.Tables, nameof(newState.Tables), log, (oldItem, newItem) => oldItem != null || newItem.Columns?.Any() == true));
            result.AddRange(GenerateScriptCompareChanges(oldState, newState, db => db.MaterializedViews, nameof(newState.MaterializedViews), log));
            result.AddRange(GenerateScriptCompareChanges(oldState, newState, db => db.ContinuousExports, nameof(newState.ContinuousExports), log));
            result.AddRange(GenerateScriptCompareChanges(oldState, newState, db => db.Functions, nameof(newState.Functions), log));
            result.AddRange(GenerateScriptCompareChanges(oldState, newState, db => db.ExternalTables, nameof(newState.ExternalTables), log));

            if (newState.EntityGroups.Any())
            {
                List<IChange> changes = GenerateEntityGroupChanges(oldState, newState, name);
                if (changes.Any())
                {
                    log.LogInformation($"Detected changes for Entity Groups: {changes.Count}");
                    result.Add(new Heading("Entity Groups"));
                    result.AddRange(changes);
                }
            }


            return result;
        }

        private static IEnumerable<IChange> GenerateDeletions(Database oldState, Deletions deletions, ILogger log)
        {
            var scripts = new List<IChange>();
            scripts.AddRange(deletions.Tables.Where(oldState.Tables.ContainsKey).Select(itm => GenerateDeletionChange(itm, "table")));
            var colDel = deletions.Columns
                .Select(itm => itm.Split('.'))
                .Select(itm => new { Table = itm[0], Column = itm[1] })
                .Where(itm => oldState.Tables.ContainsKey(itm.Table) && oldState.Tables[itm.Table].Columns.ContainsKey(itm.Column))
                .Select(itm => GenerateDeletionChange($"{itm.Table}.{itm.Column}", "column"))
                .ToList();
            scripts.AddRange(colDel);
            scripts.AddRange(deletions.Functions.Where(oldState.Functions.ContainsKey).Select(itm => GenerateDeletionChange(itm, "function")));
            scripts.AddRange(deletions.ExternalTables.Where(oldState.ExternalTables.ContainsKey).Select(itm => GenerateDeletionChange(itm, "external table")));
            scripts.AddRange(deletions.MaterializedViews.Where(oldState.MaterializedViews.ContainsKey).Select(itm => GenerateDeletionChange(itm, "materialized-view")));
            scripts.AddRange(deletions.ContinuousExports.Where(oldState.ContinuousExports.ContainsKey).Select(itm => GenerateDeletionChange(itm, "continuous-export")));

            if (scripts.Any())
            {
                scripts.Insert(0, new Heading("Deletions"));
            }
            return scripts;
        }

        public static IChange GenerateDeletionChange(string entityName, string entityType)
        {
            return new DeletionChange(entityName, entityType);
        }

        private static List<IChange> GeneratePermissionChanges(Database oldState, Database newState, string name, ILogger log)
        {
            var result = new List<IChange>();
            var permissionChanges = new List<IChange>
            {
                new PermissionChange(name, "Admins", oldState.Admins, newState.Admins),
                new PermissionChange(name, "UnrestrictedViewers", oldState.UnrestrictedViewers, newState.UnrestrictedViewers),
                new PermissionChange(name, "Users", oldState.Users, newState.Users),
                new PermissionChange(name, "Viewers",oldState.Viewers, newState.Viewers),
                new PermissionChange(name, "Monitors", oldState.Monitors, newState.Monitors),
                new PermissionChange(name, "Ingestors", oldState.Ingestors, newState.Ingestors),
            }.Where(itm => itm.Scripts.Any()).ToList();

            if (permissionChanges.Any())
            {
                log.LogInformation($"Detected {permissionChanges.Count} permission changes");
                permissionChanges.Insert(0,new Heading("Permissions"));
                
            }

            return permissionChanges;
        }

        private static List<IChange> GenerateEntityGroupChanges(Database oldState, Database newState, string name)
        {
            var changes = new List<IChange>();
            var existingEntityGroups = oldState?.EntityGroups ?? new Dictionary<string, List<Entity>>();
            foreach (var group in newState.EntityGroups)
            {
                var existing = existingEntityGroups.ContainsKey(group.Key) ? existingEntityGroups[group.Key] : null;
                var change = new EntityGroupChange(name, group.Key, existing, group.Value);
                if (change.Scripts.Any())
                {
                    changes.Add(change);
                }
            }

            return changes;
        }



        private static List<IChange> GenerateScriptCompareChanges<T>(Database oldState, Database newState,Func<Database,Dictionary<string,T>> entitySelector,string entityName, ILogger log, Func<T?,T,bool> validator = null) where T: IKustoBaseEntity
        {
            var tmp = new List<IChange>();
            var existing = entitySelector(oldState) ?? new Dictionary<string, T>();
            var newItems = entitySelector(newState) ?? new Dictionary<string, T>();


            log.LogInformation($"Existing {entityName}: {string.Join(", ", existing.Keys)}");

            foreach (var item in newItems)
            {
                var existingOldItem = existing.ContainsKey(item.Key) ? existing[item.Key] : default(T);
                if(validator != null && !validator(existingOldItem, item.Value))
                {
                    log.LogInformation($"Skipping {entityName} {item.Key} as it failed validation");
                    continue;
                }
                if (existing.ContainsKey(item.Key))
                {
                    var change = new ScriptCompareChange(item.Key, existing[item.Key], item.Value);
                    log.LogInformation($"{item.Key} already exists, created {change.Scripts.Count} script to apply the diffs");
                    tmp.Add(change);
                }
                else
                {
                    var change = new ScriptCompareChange(item.Key, null, item.Value);
                    log.LogInformation($"{item.Key} doesn't exist, created {change.Scripts.Count} scripts to create it.");
                    tmp.Add(change);
                }
            }

            tmp = tmp.Where(itm => itm.Scripts?.Any() == true).ToList();

            if(tmp.Count > 0)
            {
                tmp.Insert(0, new Heading(entityName));
            }

            return tmp;
        }

        public static List<IChange> GenerateFollowerChanges(FollowerDatabase oldState, FollowerDatabase newState, ILogger log)
        {


            List<IChange> result =
            [
                .. GenerateFollowerCachingChanges(oldState, newState, db => db.Tables, "Table", "table"),
                .. GenerateFollowerCachingChanges(oldState, newState, db => db.MaterializedViews, "MV", "materialized-view"),

            ];

            if (oldState.Permissions.ModificationKind != newState.Permissions.ModificationKind)
            {
                var kind = newState.Permissions.ModificationKind.ToString().ToLower();
                result.Add(new BasicChange("FollowerDatabase", "PermissionsModificationKind", $"From {oldState.Permissions.ModificationKind} to {newState.Permissions.ModificationKind}", new List<DatabaseScriptContainer>
                {
                    new DatabaseScriptContainer(new DatabaseScript($".alter follower database {newState.DatabaseName} principals-modification-kind = {kind}", 0), "FollowerChangePolicyModificationKind")
                }));
            }
            if (oldState.Cache.ModificationKind != newState.Cache.ModificationKind)
            {
                var kind = newState.Cache.ModificationKind.ToString().ToLower();
                result.Add(new BasicChange("FollowerDatabase", "ChangeModificationKind", $"From {oldState.Cache.ModificationKind} to {newState.Cache.ModificationKind}", new List<DatabaseScriptContainer>
                {
                    new DatabaseScriptContainer(new DatabaseScript($".alter follower database {newState.DatabaseName} caching-policies-modification-kind = {kind}", 0), "FollowerChangePolicyModificationKind")
                }));
            }

            if (oldState.Cache.DefaultHotCache != newState.Cache.DefaultHotCache)
            {
                if (newState.Cache.DefaultHotCache != null)
                {
                    result.Add(new BasicChange("FollowerDatabase", "ChangeDefaultHotCache", $"From {oldState.Cache.DefaultHotCache} to {newState.Cache.DefaultHotCache}", new List<DatabaseScriptContainer>
                    {
                        new DatabaseScriptContainer(new DatabaseScript($".alter follower database {newState.DatabaseName} policy caching hot = {newState.Cache.DefaultHotCache}", 0), "FollowerChangeDefaultHotCache")
                    }));
                }
                else
                {
                    result.Add(new BasicChange("FollowerDatabase", "DeleteDefaultHotCache", $"Remove Default Hot Cache", new List<DatabaseScriptContainer>
                    {
                        new DatabaseScriptContainer(new DatabaseScript($".delete follower database {newState.DatabaseName} policy caching", 0), "FollowerDeleteDefaultHotCache")
                    }));
                }
            }

            return result;

        }

        private static List<IChange> GenerateFollowerCachingChanges(FollowerDatabase oldState, FollowerDatabase newState, Func<FollowerCache, Dictionary<string,string>> selector, string type, string kustoType)
        {
            var result = new List<IChange>();
            var oldEntities = selector(oldState.Cache);
            var newEntities = selector(newState.Cache);


            var removedPolicyScripts = oldEntities.Keys.Except(newEntities.Keys)
                .Select(itm =>
                new
                {
                    Name = itm,
                    Script = new DatabaseScriptContainer(new DatabaseScript($".delete follower database {newState.DatabaseName} {kustoType} {itm} policy caching", 0), $"FollowerDelete{type}CachingPolicies")
                })
                .ToList();
            var changedPolicyScripts = newEntities
                    .Where(itm => oldEntities.ContainsKey(itm.Key) == false
                                  || oldEntities[itm.Key] != itm.Value)
                    .Select(itm => new
                    {
                        Name = itm.Key,
                        From = oldEntities.ContainsKey(itm.Key) ? oldEntities[itm.Key] : "default",
                        To = itm.Value,
                        Script = new DatabaseScriptContainer(new DatabaseScript($".alter follower database {newState.DatabaseName} {kustoType} {itm.Key} policy caching hot = {itm.Value}", 0), $"FollowerChange{type}CachingPolicies")
                    })
                    .ToList();

            if (removedPolicyScripts.Any())
            {
                result.Add(new Heading($"Deleted {type} Caching Policies"));

                var deletedPolicies = string.Join("\n", removedPolicyScripts.Select(itm => $"* {itm.Name}"));

                result.Add(new BasicChange(
                    "FollowerDatabase",
                    $"Delete{type}CachingPolicy",
                    deletedPolicies,
                    removedPolicyScripts.Select(itm => itm.Script).ToList()));
            }
            if (changedPolicyScripts.Any())
            {
                result.Add(new Heading($"Changed {type} Caching Policies"));

                var changePolicies = new StringBuilder();
                changePolicies.AppendLine($"{type} | From | To");
                foreach (var change in changedPolicyScripts)
                {
                    changePolicies.AppendLine($"{change.Name} | {change.From} | {change.To}");
                }

                result.Add(new BasicChange(
                    "FollowerDatabase",
                    $"Change{type}CachingPolicy",
                    changePolicies.ToString(),
                    changedPolicyScripts.Select(itm => itm.Script).ToList()));
            }

            return result;
        }
    }

}
