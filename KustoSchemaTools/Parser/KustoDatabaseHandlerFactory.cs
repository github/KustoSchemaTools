using KustoSchemaTools.Plugins;
using Microsoft.Extensions.Logging;

namespace KustoSchemaTools.Parser
{
    public class KustoDatabaseHandlerFactory
    {
        public KustoDatabaseHandlerFactory(ILogger<KustoDatabaseHandler> logger)
        {
            Logger = logger;
        }

        public List<IKustoBulkEntitiesLoader> Plugins { get; } = new ();

        public KustoDatabaseHandlerFactory WithPlugin(IKustoBulkEntitiesLoader plugin)
        {
            Plugins.Add(plugin);
            return this;
        }

        public KustoDatabaseHandlerFactory WithPlugin<T>() where T : IKustoBulkEntitiesLoader, new()
        {
            Plugins.Add(new T());
            return this;
        }

        public string OperationsCLuster { get; set; } = "ghdwprod.eastus";
        public ILogger<KustoDatabaseHandler> Logger { get; set; }

        public IDatabaseHandler Create(string cluster, string database)
        {
            return new KustoDatabaseHandler(OperationsCLuster, cluster, database, Logger, Plugins);
        }

    }

    
}
