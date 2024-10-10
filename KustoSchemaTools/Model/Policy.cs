using KustoSchemaTools.Changes;
using KustoSchemaTools.Helpers;
using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class Policy
    {
        public string? Retention { get; set; }
        public string? HotCache { get; set; }
        public PartitioningPolicy? Partitioning { get; set; }
        public string? RowLevelSecurity { get; set; }


        public List<DatabaseScriptContainer> CreateScripts(string name, string entity)
        {
            var scripts = new List<DatabaseScriptContainer>();
            if (Retention != null)
            {
                scripts.Add(new DatabaseScriptContainer("SoftDelete", 60, $".alter-merge {entity} {name} policy retention softdelete={Retention}"));
            }
            if (HotCache != null)
            {
                scripts.Add(new DatabaseScriptContainer("HotCache", 70, $".alter {entity} {name} policy caching hot={HotCache}"));
            }
          
            if (!string.IsNullOrEmpty(RowLevelSecurity))
            {
                scripts.Add(new DatabaseScriptContainer("RowLevelSecurity", 57, $".alter {entity} {name} policy row_level_security enable ```{RowLevelSecurity}```"));
            }
            else
            {
                scripts.Add(new DatabaseScriptContainer("RowLevelSecurity", 52, $".delete {entity} {name} policy row_level_security"));
            }

            if (Partitioning != null)
            {
                scripts.Add(Partitioning.CreateScript(name, entity));
            }
            return scripts;
        }
    }

    public class TablePolicy : Policy
    {

        public List<UpdatePolicy>? UpdatePolicies { get; set; }
        public bool RestrictedViewAccess { get; set; } = false;
        public List<DatabaseScriptContainer> CreateScripts(string name)
        {
            var scripts = new List<DatabaseScriptContainer>();
            scripts.AddRange(base.CreateScripts(name, "table"));
            if (UpdatePolicies != null)
            {
                var policies = JsonConvert.SerializeObject(UpdatePolicies, Serialization.JsonPascalCase);
                var upPriority = UpdatePolicies.Any() ? 59 : 50;
                scripts.Add(new DatabaseScriptContainer("TableUpdatePolicy", upPriority, $".alter table {name} policy update ```{policies}```"));
            }

            var rvaPrio = RestrictedViewAccess ? 58 : 51;
            scripts.Add(new DatabaseScriptContainer("RestrictedViewAccess", rvaPrio, $".alter table {name} policy restricted_view_access {(RestrictedViewAccess ? "true" : "false")}"));

            return scripts;

        }
    }

}
