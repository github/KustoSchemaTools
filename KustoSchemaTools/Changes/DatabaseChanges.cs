using Kusto.Language;
using KustoSchemaTools.Model;
using Microsoft.Extensions.Logging;

namespace KustoSchemaTools.Changes
{
    public class DatabaseChanges
    {
        public static List<IChange> GenerateChanges(Database oldState, Database newState, string name, Microsoft.Extensions.Logging.ILogger log)
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

            var permissionChanges = new List<IChange>
            {
                new PermissionChange(name, "Admins", oldState.Admins, newState.Admins),
                new PermissionChange(name, "UnrestrictedViewers", oldState.UnrestrictedViewers, newState.UnrestrictedViewers),
                new PermissionChange(name, "Users", oldState.Users, newState.Users),
                new PermissionChange(name, "Viewers",oldState.Viewers, newState.Viewers),
                new PermissionChange(name, "Monitors", oldState.Monitors, newState.Monitors),
                new PermissionChange(name, "Ingestors", oldState.Ingestors, newState.Ingestors),
            }.Where(itm => itm.Scripts.Any()).ToList();

            if (permissionChanges.Any() )
            {
                log.LogInformation($"Detected {permissionChanges.Count} permission changes");
                result.Add(new Heading("Permissions"));
                result.AddRange(permissionChanges);
            }

            if (newState.Tables.Any())
            {
                var tmp = new List<IChange>();
                var existingTables = oldState?.Tables ?? new Dictionary<string, Table>();
                log.LogInformation($"Existing tables: {string.Join(", ", existingTables.Keys)}");

                foreach (var table in newState.Tables)
                {
                    if (existingTables.ContainsKey(table.Key))
                    {
                        var change = new ScriptCompareChange(table.Key, existingTables[table.Key], table.Value);
                        log.LogInformation($"Table {table.Key} exists, created {change.Scripts.Count} script to apply the diffs");
                        tmp.Add(change);
                    }
                    else if (table.Value.Columns?.Count > 0)
                    {
                        var change = new ScriptCompareChange(table.Key, null, table.Value);
                        log.LogInformation($"Table {table.Key} doesn't exist, created {change.Scripts.Count} scripts to create the table");
                        tmp.Add(change);
                    }
                }
                var changes = tmp.Where(itm => itm.Scripts.Any()).ToList();
                if (changes.Any())
                {
                    log.LogInformation($"Detected changes for Tables: {changes.Count} changes with {changes.SelectMany(itm => itm.Scripts).Count()} scripts");
                    result.Add(new Heading("Tables"));
                    result.AddRange(changes);
                }
            }

            if (newState.MaterializedViews.Any())
            {
                var tmp = new List<IChange>();
                var existingMaterializedViews = oldState?.MaterializedViews ?? new Dictionary<string, MaterializedView>();
                log.LogInformation($"Existing materialized views: {string.Join(", ", existingMaterializedViews.Keys)}");

                foreach (var view in newState.MaterializedViews)
                {
                    if (existingMaterializedViews.ContainsKey(view.Key))
                    {
                        var change = new ScriptCompareChange(view.Key, existingMaterializedViews[view.Key], view.Value);
                        log.LogInformation($"Materialized view {view.Key} exists, created {change.Scripts.Count} script to apply the diffs");
                        tmp.Add(change);
                    }
                    else
                    {
                        var change = new ScriptCompareChange(view.Key, null, view.Value);
                        log.LogInformation($"Materialized view {view.Key} doesn't exist, created {change.Scripts.Count} scripts to create the view");
                        tmp.Add(change);
                    }
                }
                var changes = tmp.Where(itm => itm.Scripts.Any()).ToList();
                if (changes.Any())
                {
                    log.LogInformation($"Detected changes for MaterializedViews: {changes.Count} changes with {changes.SelectMany(itm => itm.Scripts).Count()} scripts");
                    result.Add(new Heading("MaterializedViews"));
                    result.AddRange(changes);
                }
            }

            if (newState.Functions.Any())
            {
                var tmp = new List<IChange>();
                var existingFunctions = oldState?.Functions ?? new Dictionary<string, Function>();
                log.LogInformation($"Existing functions: {string.Join(", ", existingFunctions.Keys)}");

                foreach (var function in newState.Functions)
                {
                    if (existingFunctions.ContainsKey(function.Key))
                    {
                        var existingFunction = existingFunctions[function.Key];
                        var change = new ScriptCompareChange(function.Key, existingFunction, function.Value);
                        log.LogInformation($"Function {function.Key} exists, created {change.Scripts.Count} script to apply the diffs");
                        tmp.Add(change);
                    }
                    else
                    {
                        var change = new ScriptCompareChange(function.Key, null, function.Value);
                        log.LogInformation($"Function {function.Key} doesn't exist, created {change.Scripts.Count} scripts to create the function");
                        tmp.Add(change);
                    }
                }
                var changes = tmp.Where(itm => itm.Scripts.Any()).ToList();
                if (changes.Any())
                {
                    log.LogInformation($"Detected changes for Functions: {changes.Count} changes with {changes.SelectMany(itm => itm.Scripts).Count()} scripts");
                    result.Add(new Heading("Functions"));
                    result.AddRange(changes);
                }
            }            

            if(newState.EntityGroups.Any())
            {
                var changes = new List<IChange>();
                var existingEntityGroups = oldState?.EntityGroups ?? new Dictionary<string, List<Entity>>();
                foreach (var group in newState.EntityGroups)
                {
                    var existing = existingEntityGroups.ContainsKey(group.Key) ? existingEntityGroups[group.Key] : null;
                    var change = new EntityGroupChange(name, group.Key, existing, group.Value);
                    if(change.Scripts.Any())
                    {
                        changes.Add(change);
                    }
                }
                if (changes.Any())
                {
                    log.LogInformation($"Detected changes for Entity Groups: {changes.Count}");
                    result.Add(new Heading("Entity Groups"));
                    result.AddRange(changes);
                }
            }

            if (newState.ExternalTables.Any())
            {
                var tmp = new List<IChange>();
                var existingExternalTable = oldState?.ExternalTables ?? new Dictionary<string, ExternalTable>();
                log.LogInformation($"Existing functions: {string.Join(", ", existingExternalTable.Keys)}");

                foreach (var extTable in newState.ExternalTables)
                {
                    if (existingExternalTable.ContainsKey(extTable.Key))
                    {
                        var existingFunction = existingExternalTable[extTable.Key];
                        var change = new ScriptCompareChange(extTable.Key, existingFunction, extTable.Value);
                        log.LogInformation($"Function {extTable.Key} exists, created {change.Scripts.Count} script to apply the diffs");
                        tmp.Add(change);
                    }
                    else
                    {
                        var change = new ScriptCompareChange(extTable.Key, null, extTable.Value);
                        log.LogInformation($"Function {extTable.Key} doesn't exist, created {change.Scripts.Count} scripts to create the function");
                        tmp.Add(change);
                    }
                }
                var changes = tmp.Where(itm => itm.Scripts.Any()).ToList();
                if (changes.Any())
                {
                    log.LogInformation($"Detected changes for Functions: {changes.Count} changes with {changes.SelectMany(itm => itm.Scripts).Count()} scripts");
                    result.Add(new Heading("Functions"));
                    result.AddRange(changes);
                }

            }


            return result;
        }
    }

}
