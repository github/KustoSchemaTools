using Microsoft.Extensions.Logging;

namespace KustoSchemaTools.Parser
{
    public interface IKustoClusterHandlerFactory
    {
        KustoClusterHandler Create(string clusterName, string clusterUrl);
    }
}
