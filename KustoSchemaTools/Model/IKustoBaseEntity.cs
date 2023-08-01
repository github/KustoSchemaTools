using KustoSchemaTools.Changes;

namespace KustoSchemaTools.Model
{
    public interface IKustoBaseEntity
    {
        List<DatabaseScriptContainer> CreateScripts(string name);
    }

}
