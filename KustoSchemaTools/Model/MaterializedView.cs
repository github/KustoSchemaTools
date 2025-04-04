using KustoSchemaTools.Changes;
using System.ComponentModel;
using YamlDotNet.Core;
using YamlDotNet.Serialization;

namespace KustoSchemaTools.Model
{
    public class MaterializedView : IKustoBaseEntity
    {
        public string Source { get; set; }
        public string Kind { get; set; } = "table";
        public string Folder { get; set; }
        public string DocString { get; set; }
        public string? EffectiveDateTime { get; set; }
        public string Lookback { get; set; }
        public bool? UpdateExtentsCreationTime { get; set; }
        public bool? Backfill { get; set; }
        public bool AutoUpdateSchema { get; set; } = false;
        public List<string> DimensionTables { get; set; }
        [Obsolete("Use policies instead")]
        public RetentionAndCachePolicy RetentionAndCachePolicy { get; set; } = new RetentionAndCachePolicy();
        [YamlMember(ScalarStyle = ScalarStyle.Literal)]
        public string Query { get; set; }
        [Obsolete("Use policies instead")]
        public string? RowLevelSecurity { get; set; }
        public Policy? Policies { get; set; }

        public List<DatabaseScriptContainer> CreateScripts(string name, bool isNew)
        {
            var asyncSetup = isNew && Backfill == true;


            var excludedProperties = new HashSet<string>(["Query", "Source", "Kind", "RetentionAndCachePolicy", "RowLevelSecurity", "Policies"]);
            if (!asyncSetup)
            {
                excludedProperties.Add("EffectiveDateTime");
                excludedProperties.Add("Backfill");
            }

            var scripts = new List<DatabaseScriptContainer>();
            var properties = string.Join(", ", GetType().GetProperties()
                .Where(p => p.GetValue(this) != null && excludedProperties.Contains(p.Name) == false)
                .Select(p => new {Name = p.Name, Value = p.GetValue(this) })
                .Where(p => !string.IsNullOrWhiteSpace(p.Value?.ToString()))
                .Select(p => $"{p.Name}=```{p.Value}```"));

  
            if (asyncSetup)
            {
                scripts.Add(new DatabaseScriptContainer("CreateMaterializedViewAsync", Kind == "table" ? 40 : 41, $".create async ifnotexists materialized-view with ({properties}) {name} on {Kind} {Source} {{ {Query} }}", true));
            }
            else
            {
                scripts.Add(new DatabaseScriptContainer("CreateAlterMaterializedView", Kind == "table" ? 40 : 41, $".create-or-alter materialized-view with ({properties}) {name} on {Kind} {Source} {{ {Query} }}"));
            }
            if (Policies != null)
            {
                scripts.AddRange(Policies.CreateScripts(name, "materialized-view"));
            }
           
            return scripts;
        }
    }

}
