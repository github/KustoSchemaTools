using Microsoft.Extensions.Logging;
using KustoSchemaTools.Parser;

namespace KustoSchemaTools
{
    public class KustoClusterHandlerFactory
    {
        private readonly ILoggerFactory _loggerFactory;

        public KustoClusterHandlerFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
        }

        public virtual KustoClusterHandler Create(string clusterUrl)
        {
            var client = new KustoClient(clusterUrl, _loggerFactory.CreateLogger<KustoClient>());
            var logger = _loggerFactory.CreateLogger<KustoClusterHandler>();
            return new KustoClusterHandler(client, logger);
        }
    }
}