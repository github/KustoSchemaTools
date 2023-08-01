using KustoSchemaTools.Model;

namespace KustoSchemaTools.Parser
{
    public interface IDatabaseHandler
    {
        Task<Database> LoadAsync();
        Task WriteAsync(Database database);

    }
}
