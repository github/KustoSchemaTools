using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Microsoft.Extensions.Logging;

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
            var client = KustoClientFactory.CreateCslQueryProvider(clusterUrl);
            var logger = _loggerFactory.CreateLogger<KustoClusterHandler>();
            return new KustoClusterHandler(client, logger);
        }
    }
}