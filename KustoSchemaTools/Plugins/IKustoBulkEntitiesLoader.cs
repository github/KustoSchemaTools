using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;

namespace KustoSchemaTools.Plugins
{
    public interface IKustoBulkEntitiesLoader
    {
        Task Load(Database database,  string databaseName, KustoClient client);
    }
}
