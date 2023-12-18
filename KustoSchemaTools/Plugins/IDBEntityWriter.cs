using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using Microsoft.Extensions.Logging;

namespace KustoSchemaTools.Plugins
{
    public interface IDBEntityWriter
    {
        Task WriteAsync(Database sourceDb, Database targetDb, KustoClient client, ILogger logger);
    }
}
