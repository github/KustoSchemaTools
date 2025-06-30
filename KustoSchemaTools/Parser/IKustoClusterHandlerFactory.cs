using Microsoft.Extensions.Logging;

namespace KustoSchemaTools.Parser
{
    public interface IKustoClusterHandlerFactory
    {
        KustoClusterHandler Create(string clusterUrl);
    }
}
