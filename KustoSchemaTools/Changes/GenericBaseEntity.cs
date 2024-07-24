using KustoSchemaTools.Model;

namespace KustoSchemaTools.Changes
{
    public class GenericBaseEntity : IKustoBaseEntity
    {
        public GenericBaseEntity(List<DatabaseScriptContainer> scripts)
        {
            Scripts = scripts;
        }

        public List<DatabaseScriptContainer> Scripts { get; }

        public List<DatabaseScriptContainer> CreateScripts(string name, bool isNew)
        {
            return Scripts;
        }
    }
}
