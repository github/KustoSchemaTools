using KustoSchemaTools.Changes;

namespace KustoSchemaTools.Model
{
    public class RetentionAndCachePolicy
    {
        public string? Retention { get; set; }
        public string? HotCache { get; set; }

        public List<DatabaseScriptContainer> CreateScripts(string name, string entity)
        {
            var scripts = new List<DatabaseScriptContainer>();
            if (Retention != null)
            {
                scripts.Add(new DatabaseScriptContainer("SoftDelete",60, $".alter-merge {entity} {name} policy retention softdelete={Retention}"));
            }
            if (HotCache != null)
            {
                scripts.Add(new DatabaseScriptContainer("HotCache",70,$".alter {entity} {name} policy caching hot={HotCache}"));
            }
            return scripts;
        }
    }

}
