using KustoSchemaTools.Model;

namespace KustoSchemaTools.Parser
{
    public interface IDatabaseHandler<T> where T: Database
    {
        Task<T> LoadAsync();
        Task WriteAsync(T database);

    }
}
