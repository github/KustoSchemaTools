using Kusto.Cloud.Platform.Utils;
using Kusto.Language;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
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

            // Kusto does not expose AllowMaterializedViewsWithoutRowLevelSecurity in any query output,
            // so propagate the flag from the desired state to the cluster state to avoid phantom diffs.
            foreach (var table in newState.Tables)
            {
                if (table.Value.Policies?.AllowMaterializedViewsWithoutRowLevelSecurity == true
                    && oldState.Tables.ContainsKey(table.Key)
                    && oldState.Tables[table.Key].Policies != null)
                {
                    oldState.Tables[table.Key].Policies.AllowMaterializedViewsWithoutRowLevelSecurity = true;
                }
            }

            foreach (var mv in newState.MaterializedViews)
            {
                if (mv.Value.AllowMaterializedViewsWithoutRowLevelSecurity
                    && oldState.MaterializedViews.ContainsKey(mv.Key))
                {
                    oldState.MaterializedViews[mv.Key].AllowMaterializedViewsWithoutRowLevelSecurity = true;
                }
            }

            result.AddRange(GenerateScriptCompareChanges(oldState, newState, db => db.Tables, nameof(newState.Tables), log, (oldItem, newItem) => oldItem != null || newItem.Columns?.Any() == true));
            var mvChanges = GenerateScriptCompareChanges(oldState, newState, db => db.MaterializedViews, nameof(newState.MaterializedViews), log);
            foreach(var mvChange in mvChanges)
            {                                
                var relevantChange = mvChange.Scripts.FirstOrDefault(itm => itm.Kind== "CreateMaterializedViewAsync");
                if (relevantChange == null) 
                    continue;


                var newMv = newState.MaterializedViews[mvChange.Entity];

                var specificCache = (newMv.Kind== "table" ?
                    (newState.Tables.ContainsKey(newMv.Source) ? newState.Tables[newMv.Source].Policies?.HotCache : null) :
                    (newState.MaterializedViews.ContainsKey(newMv.Source) ? newState.MaterializedViews[newMv.Source].Policies?.HotCache : null))
                    ?? newState.DefaultRetentionAndCache?.HotCache;

                if(specificCache != null && specificCache.EndsWith("d") && int.TryParse(specificCache.TrimEnd("d"), out int lookBackInDays) && DateTime.TryParse(newMv.EffectiveDateTime, out var effectiveDateTime))
                {
                    if(DateTime.UtcNow.AddDays(-lookBackInDays) < effectiveDateTime)
                    {
                        // Backfill will work
                        var validUntil = effectiveDateTime.AddDays(lookBackInDays);
                        mvChange.Comment = new Comment { FailsRollout = false, Kind = CommentKind.Note, Text = $"The materialized view {mvChange.Entity} is specified to be created with backfill configured. All required data is available in hot cache and the rollout is expected to succeed as long as it is rolled out before {validUntil:yyyy-MM-dd HH:mm}UTC. The rollout will be executed asynchronously, depending on the size of the backfill it might take a while." };
                    }
                    else
                    {
                        // Backfill will fail
                        var validUntil = DateTime.UtcNow.Date.AddDays(1-lookBackInDays);
                        mvChange.Comment = new Comment { FailsRollout = true, Kind = CommentKind.Caution, Text = $"Not all data for the backfill of {mvChange.Entity} is available hot. The backfill will fail! Please set the effective Date of the MV to {validUntil:yyyy-MM-dd} or newer." };
                    }
                }
                else
                {
                    mvChange.Comment = new Comment { FailsRollout = false, Kind = CommentKind.Warning, Text = $"The conditions for backfilling {mvChange.Entity} couldn't be validated. Please check for errors!" };
                }
            }            

            result.AddRange(mvChanges);
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



        private static List<IChange> GenerateScriptCompareChanges<T>(Database oldState, Database newState,Func<Database,Dictionary<string,T>> entitySelector,string entityName, ILogger log, Func<T?,T,bool>? validator = null) where T: IKustoBaseEntity
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
                    LogChangeResult(log, item.Key, change.Scripts.Count, alreadyExists: true);
                    tmp.Add(change);
                }
                else
                {
                    var change = new ScriptCompareChange(item.Key, null, item.Value);
                    LogChangeResult(log, item.Key, change.Scripts.Count, alreadyExists: false);
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
            if (!SupportsFollowerClusterCommands())
            {
                log.LogDebug("Skipping follower database changes because cluster-scoped follower commands cannot be executed in the current rollout context.");
                return [];
            }

            List<IChange> result =
            [
                .. GenerateFollowerCachingChanges(oldState, newState, db => db.Tables, "Table", "table"),
                .. GenerateFollowerCachingChanges(oldState, newState, db => db.MaterializedViews, "MV", "materialized-view"),

            ];

            if (oldState.Permissions.ModificationKind != newState.Permissions.ModificationKind)
            {
                var kind = newState.Permissions.ModificationKind.ToString().ToLower();
                result.Add(new BasicChange("FollowerDatabase", "PermissionsModificationKind", $" Change Permission-Modification-Kind from {oldState.Permissions.ModificationKind} to {newState.Permissions.ModificationKind}", new List<DatabaseScriptContainer>
                {
                    new DatabaseScriptContainer(new DatabaseScript($".alter follower database {newState.DatabaseName.BracketIfIdentifier()} principals-modification-kind = {kind}", 0), "FollowerChangePolicyModificationKind")
                }));
            }
            if (oldState.Cache.ModificationKind != newState.Cache.ModificationKind)
            {
                var kind = newState.Cache.ModificationKind.ToString().ToLower();
                result.Add(new BasicChange("FollowerDatabase", "ChangeModificationKind", $"Change Caching-Modification-Kind from {oldState.Cache.ModificationKind} to {newState.Cache.ModificationKind}", new List<DatabaseScriptContainer>
                {
                    new DatabaseScriptContainer(new DatabaseScript($".alter follower database {newState.DatabaseName.BracketIfIdentifier()} caching-policies-modification-kind = {kind}", 0), "FollowerChangePolicyModificationKind")
                }));
            }

            if (oldState.Cache.DefaultHotCache != newState.Cache.DefaultHotCache)
            {
                if (newState.Cache.DefaultHotCache != null)
                {
                    result.Add(new BasicChange("FollowerDatabase", "ChangeDefaultHotCache", $"From {oldState.Cache.DefaultHotCache} to {newState.Cache.DefaultHotCache}", new List<DatabaseScriptContainer>
                    {
                        new DatabaseScriptContainer(new DatabaseScript($".alter follower database {newState.DatabaseName.BracketIfIdentifier()} policy caching hot = {newState.Cache.DefaultHotCache}", 0), "FollowerChangeDefaultHotCache")
                    }));
                }
                else
                {
                    result.Add(new BasicChange("FollowerDatabase", "DeleteDefaultHotCache", $"Remove Default Hot Cache", new List<DatabaseScriptContainer>
                    {
                        new DatabaseScriptContainer(new DatabaseScript($".delete follower database {newState.DatabaseName.BracketIfIdentifier()} policy caching", 0), "FollowerDeleteDefaultHotCache")
                    }));
                }
            }

            foreach(var script in result.SelectMany(itm => itm.Scripts))
            {
                var code = KustoCode.Parse(script.Script.Text);
                var diagnostics = code.GetDiagnostics();
                script.IsValid = !diagnostics.Any();
                script.Diagnostics = diagnostics.Any()
                    ? diagnostics.Select(diagnostic => new ScriptDiagnostic
                    {
                        Start = diagnostic.Start,
                        End = diagnostic.End,
                        Description = diagnostic.Description
                    }).ToList()
                    : null;
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
                    Script = new DatabaseScriptContainer(new DatabaseScript($".delete follower database {newState.DatabaseName.BracketIfIdentifier()} {kustoType} {itm} policy caching", 0), $"FollowerDelete{type}CachingPolicies")
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
                        Script = new DatabaseScriptContainer(new DatabaseScript($".alter follower database {newState.DatabaseName.BracketIfIdentifier()} {kustoType} {itm.Key} policy caching hot = {itm.Value}", 0), $"FollowerChange{type}CachingPolicies")
                    })
                    .ToList();

            if (removedPolicyScripts.Any())
            {
                var deletePolicySb = new StringBuilder();
                deletePolicySb.AppendLine($"## Delete {type} Caching Policies");
                foreach (var change in removedPolicyScripts)
                {
                    deletePolicySb.AppendLine($"* {change.Name}");
                }

                result.Add(new BasicChange(
                    "FollowerDatabase",
                    $"Delete{type}CachingPolicy",
                    deletePolicySb.ToString(),
                    removedPolicyScripts.Select(itm => itm.Script).ToList()));
            }
            if (changedPolicyScripts.Any())
            {
                var changePolicies = new StringBuilder();
                changePolicies.AppendLine($"## Changed {type} Caching Policies");
                changePolicies.AppendLine($"{type} | From | To");
                changePolicies.AppendLine("--|--|--");
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

        private static bool SupportsFollowerClusterCommands()
        {
            var disableFlag = Environment.GetEnvironmentVariable("DISABLE_FOLLOWER_COMMANDS");

            if (disableFlag == null)
            {
                return true;
            }

            var isDisabled = bool.TryParse(disableFlag, out var parsed)
                ? parsed
                : string.Equals(disableFlag, "1", StringComparison.OrdinalIgnoreCase)
                  || string.Equals(disableFlag, "yes", StringComparison.OrdinalIgnoreCase)
                  || string.Equals(disableFlag, "true", StringComparison.OrdinalIgnoreCase);

            return !isDisabled;
        }

        private static void LogChangeResult(ILogger log, string entityKey, int scriptCount, bool alreadyExists)
        {
            var level = scriptCount > 0 ? LogLevel.Information : LogLevel.Debug;
            var scriptsLabel = scriptCount == 1 ? "script" : "scripts";
            var message = alreadyExists
                ? $"{entityKey} already exists, created {scriptCount} {scriptsLabel} to apply the diffs."
                : $"{entityKey} doesn't exist, created {scriptCount} {scriptsLabel} to create it.";

            log.Log(level, message);
        }
    }

}
