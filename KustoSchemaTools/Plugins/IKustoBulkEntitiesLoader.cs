using FlowLoop.Shared.Kusto;
using KustoSchemaRollout.Model;

namespace KustoSchemaTools.Plugins
{
    public interface IKustoBulkEntitiesLoader
    {
        Task Load(Database database,  string databaseName, KustoClient client);
    }
}
