using Microsoft.Extensions.Logging;
using KustoSchemaTools.Parser;

namespace KustoSchemaTools
{
    public class KustoClusterHandlerFactory : IKustoClusterHandlerFactory
    {
        private readonly ILoggerFactory _loggerFactory;

        public KustoClusterHandlerFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
        }

        public virtual KustoClusterHandler Create(string clusterName, string clusterUrl)
        {
            var client = new KustoClient(clusterUrl);
            var logger = _loggerFactory.CreateLogger<KustoClusterHandler>();
            return new KustoClusterHandler(client.AdminClient, logger, clusterName, clusterUrl);
        }
    }
}