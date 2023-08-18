using KustoSchemaTools.Changes;
using KustoSchemaTools.Helpers;
using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class Table : IKustoBaseEntity
    {

        public string Folder { get; set; }
        public RetentionAndCachePolicy RetentionAndCachePolicy { get; set; } = new RetentionAndCachePolicy();
        public Dictionary<string, string> Columns { get; set; }
        public List<UpdatePolicy> UpdatePolicies { get; set; }
        public List<DatabaseScript> Scripts { get; set; }
        public string DocString { get; set; }
        public bool RestrictedViewAccess { get; set; } = false;
        public string? RowLevelSecurity { get; set; }

        public List<DatabaseScriptContainer> CreateScripts(string name)
        {
            var scripts = new List<DatabaseScriptContainer>();
            if (Columns != null)
            {
                var properties = string.Join(", ", GetType().GetProperties()
                    .Where(p => p.GetValue(this) != null && (p.Name == "Folder" || p.Name == "DocString"))
                    .Select(p => $"{p.Name}=\"{p.GetValue(this)}\""));

                scripts.Add(new DatabaseScriptContainer("CreateMergeTable", 30, $".create-merge table {name} ({string.Join(", ", Columns.Select(c => $"{c.Key}:{c.Value}"))})"));
            }
            else
            {

            }

            scripts.Add(new DatabaseScriptContainer("TableFolder", 31, $".alter table {name} folder '{Folder}'"));
            scripts.Add(new DatabaseScriptContainer("TableDocString", 31, $".alter table {name} docstring '{DocString}'"));
            var ups = UpdatePolicies ?? new List<UpdatePolicy>();
            var policies = JsonConvert.SerializeObject(ups, Serialization.JsonPascalCase);
            var upPriority = ups.Any() ? 59 : 50;
            scripts.Add(new DatabaseScriptContainer("TableUpdatePolicy", upPriority, $".alter table {name} policy update ```{policies}```"));

            if (RetentionAndCachePolicy != null)
            {
                scripts.AddRange(RetentionAndCachePolicy.CreateScripts(name, "table"));
            }
            if (Scripts != null)
            {
                scripts.AddRange(Scripts.Select(itm => new DatabaseScriptContainer(itm, "DatabaseScript")));
            }

            var rvaPrio = RestrictedViewAccess ? 58 : 51;
            scripts.Add(new DatabaseScriptContainer("RestrictedViewAccess", rvaPrio, $".alter table {name} policy restricted_view_access {(RestrictedViewAccess ? "true" : "false")}"));


            if (!string.IsNullOrEmpty(RowLevelSecurity))
            {
                scripts.Add(new DatabaseScriptContainer("RowLevelSecurity", 57, $".alter table {name} policy row_level_security enable \"{RowLevelSecurity}\""));
            }
            else
            {
                scripts.Add(new DatabaseScriptContainer("RowLevelSecurity", 52, $".alter table {name} policy row_level_security disable"));
            }
            return scripts;
        }
    }



}
